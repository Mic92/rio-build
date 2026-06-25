//! Minimal EC2 query-protocol client for the two read-only operations
//! the scheduler needs: `DescribeInstanceTypes` (boot-time catalog
//! ceilings) and `DescribeSpotPriceHistory` (the hourly cost poll).
//!
//! `aws-sdk-ec2` code-generates ~600 operations; we use 2. The crate
//! is one of the slowest units in the workspace build and one of the
//! largest contributors to the scheduler binary's `.text`. The EC2 API
//! is the legacy query protocol — `POST /` with a form-urlencoded
//! `Action=…&Version=…` body, SigV4-signed, XML response — so a
//! hand-rolled client for two paginated calls fits against deps the
//! scheduler already links (reqwest, aws-sigv4, aws-config) plus
//! quick-xml. xtask keeps the full SDK (AMI lifecycle,
//! VPC teardown); that crate is dev-only and not on the hot build
//! path.
//!
//! Shapes are hand-transcribed from the botocore service model
//! (`botocore/data/ec2/2016-11-15/service-2.json`) and trimmed to the
//! fields [`super::catalog`] / [`super::cost`] actually read.

use std::time::SystemTime;

use anyhow::{Context, Result, bail};
use aws_credential_types::provider::ProvideCredentials;
use aws_sigv4::http_request::{SignableBody, SignableRequest, SigningSettings, sign};
use aws_sigv4::sign::v4;
use quick_xml::events::Event;
use quick_xml::reader::Reader;

const API_VERSION: &str = "2016-11-15";

type Params = Vec<(String, String)>;

fn p(k: &str, v: impl Into<String>) -> (String, String) {
    (k.to_owned(), v.into())
}

/// Thin EC2 client: region + credentials provider + a pooled reqwest
/// client. Built once per long-lived poller.
pub struct Client {
    region: String,
    host: String,
    creds: aws_credential_types::provider::SharedCredentialsProvider,
    http: reqwest::Client,
}

impl Client {
    /// Build from the same `aws_config::from_env()` chain the S3 client
    /// uses (IRSA in-cluster, profile/env locally). Panics if the
    /// config has no region — the caller already gated on
    /// `hw_cost_source == Spot`, which implies an AWS deployment.
    pub fn new(conf: &aws_config::SdkConfig) -> Self {
        let region = conf
            .region()
            .expect("ec2: SdkConfig has no region")
            .to_string();
        Self {
            host: format!("ec2.{region}.amazonaws.com"),
            region,
            creds: conf
                .credentials_provider()
                .expect("ec2: SdkConfig has no credentials provider"),
            http: reqwest::Client::new(),
        }
    }

    /// `DescribeInstanceTypes`, all pages, no filter — the full
    /// regional catalog (see [`super::catalog::fetch_catalog`] for why
    /// no server-side filter). On any page error returns the error;
    /// the caller maps that to "fall to global".
    pub async fn describe_instance_types(&self) -> Result<Vec<InstanceTypeInfo>> {
        self.paginate(
            |q| q.extend([p("Action", "DescribeInstanceTypes"), p("MaxResults", "100")]),
            parse_instance_types,
        )
        .await
    }

    /// `DescribeSpotPriceHistory` for `instance_types` since
    /// `start_epoch_secs`, `Linux/UNIX` only, all pages.
    pub async fn describe_spot_price_history(
        &self,
        instance_types: &[String],
        start_epoch_secs: i64,
    ) -> Result<Vec<SpotPrice>> {
        let start = epoch_to_rfc3339(start_epoch_secs.max(0));
        self.paginate(
            |q| {
                q.extend([
                    p("Action", "DescribeSpotPriceHistory"),
                    p("StartTime", start.clone()),
                    p("ProductDescription.1", "Linux/UNIX"),
                    p("MaxResults", "1000"),
                ]);
                // EC2 query protocol: 1-indexed flattened list members.
                for (i, t) in instance_types.iter().enumerate() {
                    q.push((format!("InstanceType.{}", i + 1), t.clone()));
                }
            },
            parse_spot_history,
        )
        .await
    }

    /// Drive a `NextToken`-paginated query: `build` populates the
    /// per-page params (Version/NextToken added here), `parse` returns
    /// `(rows, nextToken)`. EC2 signals last-page as either absent or
    /// empty-string nextToken depending on the operation.
    async fn paginate<T>(
        &self,
        build: impl Fn(&mut Params),
        parse: impl Fn(&str) -> Result<(Vec<T>, Option<String>)>,
    ) -> Result<Vec<T>> {
        let mut out = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut params = vec![p("Version", API_VERSION)];
            build(&mut params);
            if let Some(t) = &token {
                params.push(p("NextToken", t.clone()));
            }
            let xml = self.call(params).await?;
            let (page, next) = parse(&xml)?;
            out.extend(page);
            match next.filter(|t| !t.is_empty()) {
                Some(t) => token = Some(t),
                None => return Ok(out),
            }
        }
    }

    /// SigV4-sign and POST one form-encoded EC2 query. Returns the raw
    /// XML body on 2xx; on non-2xx, surfaces the `<Error><Message>` if
    /// present.
    async fn call(&self, params: Params) -> Result<String> {
        let body = url::form_urlencoded::Serializer::new(String::new())
            .extend_pairs(&params)
            .finish();
        let endpoint = format!("https://{}/", self.host);

        let creds = self
            .creds
            .provide_credentials()
            .await
            .context("ec2: resolving AWS credentials")?;
        let identity = creds.into();
        let sp = v4::SigningParams::builder()
            .identity(&identity)
            .region(&self.region)
            .name("ec2")
            .time(SystemTime::now())
            .settings(SigningSettings::default())
            .build()
            .context("ec2: building signing params")?;
        let mut headers = vec![
            ("host", self.host.clone()),
            (
                "content-type",
                "application/x-www-form-urlencoded; charset=utf-8".to_owned(),
            ),
        ];
        let signable = SignableRequest::new(
            "POST",
            &endpoint,
            headers.iter().map(|(k, v)| (*k, v.as_str())),
            SignableBody::Bytes(body.as_bytes()),
        )
        .context("ec2: building signable request")?;
        let (instr, _) = sign(signable, &sp.into())
            .context("ec2: SigV4 signing")?
            .into_parts();
        headers.extend(instr.headers().map(|(k, v)| (k, v.to_owned())));

        let mut req = self.http.post(&endpoint).body(body);
        for (k, v) in &headers {
            req = req.header(*k, v);
        }
        let resp = req.send().await.context("ec2: HTTP send")?;
        let status = resp.status();
        let text = resp.text().await.context("ec2: reading body")?;
        if !status.is_success() {
            bail!("ec2 {}: {status}: {}", self.host, error_message(&text));
        }
        Ok(text)
    }
}

/// The `<Error><Code>…</Code><Message>…</Message></Error>` envelope.
/// Best-effort — falls back to the raw body.
fn error_message(xml: &str) -> String {
    let mut r = Reader::from_str(xml);
    r.config_mut().trim_text(true);
    let (mut tag, mut code, mut msg) = (Vec::new(), String::new(), String::new());
    while let Ok(ev) = r.read_event() {
        match ev {
            Event::Start(e) => tag = e.local_name().as_ref().to_vec(),
            Event::Text(t) if tag == b"Code" => code = t.decode().unwrap_or_default().into(),
            Event::Text(t) if tag == b"Message" => msg = t.decode().unwrap_or_default().into(),
            Event::Eof => break,
            _ => {}
        }
    }
    if code.is_empty() && msg.is_empty() {
        xml.to_owned()
    } else {
        format!("{code}: {msg}")
    }
}

/// Projection of `InstanceTypeInfo` onto the fields
/// [`super::catalog::from_instance_type_info`] reads. Everything else
/// the API returns is dropped at parse time.
#[derive(Debug, Clone, Default)]
pub struct InstanceTypeInfo {
    pub instance_type: Option<String>,
    pub default_vcpus: Option<i32>,
    pub memory_mib: Option<i64>,
    /// `processorInfo.supportedArchitectures` — raw EC2 strings
    /// (`x86_64`, `arm64`, …); mapped to k8s arch in catalog.rs.
    pub supported_architectures: Vec<String>,
    pub manufacturer: Option<String>,
    /// `instanceStorageInfo.totalSizeInGB`; `None` on ebs-only types
    /// (the API omits the whole `instanceStorageInfo` block).
    pub total_storage_gb: Option<i64>,
}

/// One `spotPriceHistorySet/item` row — only the two fields the cost
/// poller reads.
#[derive(Debug, Clone, Default)]
pub struct SpotPrice {
    pub instance_type: Option<String>,
    pub spot_price: Option<String>,
}

fn parse_instance_types(xml: &str) -> Result<(Vec<InstanceTypeInfo>, Option<String>)> {
    walk_set(
        xml,
        b"instanceTypeSet",
        |it: &mut InstanceTypeInfo, path, txt| match path {
            [b"instanceType"] => it.instance_type = Some(txt.into()),
            [b"vCpuInfo", b"defaultVCpus"] => it.default_vcpus = txt.parse().ok(),
            [b"memoryInfo", b"sizeInMiB"] => it.memory_mib = txt.parse().ok(),
            [b"processorInfo", b"supportedArchitectures", b"item"] => {
                it.supported_architectures.push(txt.into());
            }
            [b"processorInfo", b"manufacturer"] => it.manufacturer = Some(txt.into()),
            [b"instanceStorageInfo", b"totalSizeInGB"] => {
                it.total_storage_gb = txt.parse().ok();
            }
            _ => {}
        },
    )
}

fn parse_spot_history(xml: &str) -> Result<(Vec<SpotPrice>, Option<String>)> {
    walk_set(
        xml,
        b"spotPriceHistorySet",
        |sp: &mut SpotPrice, path, txt| match path {
            [b"instanceType"] => sp.instance_type = Some(txt.into()),
            [b"spotPrice"] => sp.spot_price = Some(txt.into()),
            _ => {}
        },
    )
}

/// Walk the `<Response><set><item>…` shape every paginated EC2 list
/// response uses: for each `<set>/<item>` child, `on_leaf(row,
/// item_relative_path, text)` fires per text node; the row is pushed
/// on `</item>`. Also captures the top-level `nextToken`. EC2
/// responses carry payload only in element text (no attributes), so a
/// path-tagged text stream is sufficient.
fn walk_set<T: Default>(
    xml: &str,
    set: &[u8],
    mut on_leaf: impl FnMut(&mut T, &[&[u8]], &str),
) -> Result<(Vec<T>, Option<String>)> {
    let mut r = Reader::from_str(xml);
    r.config_mut().trim_text(true);
    let mut path: Vec<Vec<u8>> = Vec::new();
    let mut out = Vec::new();
    let mut next = None;
    let mut cur = T::default();
    loop {
        match r.read_event()? {
            Event::Start(e) => path.push(e.local_name().as_ref().to_vec()),
            Event::End(_) => {
                if matches!(&view(&path)[..], [_, s, b"item"] if *s == set) {
                    out.push(std::mem::take(&mut cur));
                }
                path.pop();
            }
            Event::Text(t) => {
                let txt = t.decode()?;
                match &view(&path)[..] {
                    [_, b"nextToken"] => next = Some(txt.into_owned()),
                    [_, s, b"item", rest @ ..] if *s == set => on_leaf(&mut cur, rest, &txt),
                    _ => {}
                }
            }
            Event::Eof => return Ok((out, next)),
            _ => {}
        }
    }
}

/// `&[Vec<u8>] → Vec<&[u8]>` so the path can be slice-pattern-matched
/// (`[_, b"nextToken"]`). Tiny per-event alloc; the boot-time catalog
/// is ~25k leaves and the spot poll is smaller still.
fn view(path: &[Vec<u8>]) -> Vec<&[u8]> {
    path.iter().map(Vec::as_slice).collect()
}

/// `YYYY-MM-DDThh:mm:ssZ` from a Unix epoch second. EC2 wants ISO
/// 8601 UTC for `StartTime`. Howard Hinnant's `civil_from_days` —
/// dependency-free; we only need second precision and dates after
/// 1970, so the proleptic-Gregorian edge cases don't apply.
fn epoch_to_rfc3339(secs: i64) -> String {
    let days = secs.div_euclid(86_400);
    let sod = secs.rem_euclid(86_400);
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        y,
        m,
        d,
        sod / 3600,
        (sod % 3600) / 60,
        sod % 60
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Real `DescribeInstanceTypes` response excerpt (trimmed): one
    /// nvme arm64 row, one ebs-only x86_64 row, plus a nextToken. The
    /// parser must pick exactly the fields catalog.rs reads and ignore
    /// the rest.
    #[test]
    fn parse_instance_types_excerpt() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<DescribeInstanceTypesResponse xmlns="http://ec2.amazonaws.com/doc/2016-11-15/">
  <requestId>abc</requestId>
  <instanceTypeSet>
    <item>
      <instanceType>c8gd.2xlarge</instanceType>
      <currentGeneration>true</currentGeneration>
      <vCpuInfo><defaultVCpus>8</defaultVCpus><defaultCores>8</defaultCores></vCpuInfo>
      <memoryInfo><sizeInMiB>16384</sizeInMiB></memoryInfo>
      <processorInfo>
        <supportedArchitectures><item>arm64</item></supportedArchitectures>
        <manufacturer>AWS</manufacturer>
      </processorInfo>
      <instanceStorageInfo>
        <totalSizeInGB>474</totalSizeInGB>
        <disks><item><sizeInGB>474</sizeInGB></item></disks>
      </instanceStorageInfo>
    </item>
    <item>
      <instanceType>c7a.large</instanceType>
      <vCpuInfo><defaultVCpus>2</defaultVCpus></vCpuInfo>
      <memoryInfo><sizeInMiB>4096</sizeInMiB></memoryInfo>
      <processorInfo>
        <supportedArchitectures><item>x86_64</item></supportedArchitectures>
        <manufacturer>AMD</manufacturer>
      </processorInfo>
    </item>
  </instanceTypeSet>
  <nextToken>AAPage2</nextToken>
</DescribeInstanceTypesResponse>"#;
        let (rows, next) = parse_instance_types(xml).unwrap();
        assert_eq!(next.as_deref(), Some("AAPage2"));
        assert_eq!(rows.len(), 2);
        let a = &rows[0];
        assert_eq!(a.instance_type.as_deref(), Some("c8gd.2xlarge"));
        assert_eq!(a.default_vcpus, Some(8));
        assert_eq!(a.memory_mib, Some(16384));
        assert_eq!(a.supported_architectures, vec!["arm64"]);
        assert_eq!(a.manufacturer.as_deref(), Some("AWS"));
        assert_eq!(a.total_storage_gb, Some(474));
        let b = &rows[1];
        assert_eq!(b.instance_type.as_deref(), Some("c7a.large"));
        assert_eq!(b.total_storage_gb, None, "ebs-only: block absent → None");
    }

    #[test]
    fn parse_spot_history_excerpt() {
        let xml = r#"<DescribeSpotPriceHistoryResponse>
  <spotPriceHistorySet>
    <item>
      <instanceType>c7a.large</instanceType>
      <productDescription>Linux/UNIX</productDescription>
      <spotPrice>0.031200</spotPrice>
      <availabilityZone>us-east-1a</availabilityZone>
    </item>
    <item>
      <instanceType>m7a.large</instanceType>
      <spotPrice>0.400000</spotPrice>
    </item>
  </spotPriceHistorySet>
  <nextToken></nextToken>
</DescribeSpotPriceHistoryResponse>"#;
        let (rows, next) = parse_spot_history(xml).unwrap();
        // EC2 returns empty-string nextToken on the last spot-history
        // page; trim_text collapses it to absent.
        assert!(next.as_deref().unwrap_or("").is_empty());
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].instance_type.as_deref(), Some("c7a.large"));
        assert_eq!(rows[0].spot_price.as_deref(), Some("0.031200"));
        assert_eq!(rows[1].instance_type.as_deref(), Some("m7a.large"));
    }

    #[test]
    fn error_envelope() {
        let xml = r#"<Response><Errors><Error>
            <Code>AuthFailure</Code>
            <Message>AWS was not able to validate the provided access credentials</Message>
        </Error></Errors></Response>"#;
        let m = error_message(xml);
        assert!(m.starts_with("AuthFailure:"), "{m}");
    }

    #[test]
    fn rfc3339() {
        assert_eq!(epoch_to_rfc3339(0), "1970-01-01T00:00:00Z");
        assert_eq!(epoch_to_rfc3339(1_700_000_000), "2023-11-14T22:13:20Z");
        assert_eq!(epoch_to_rfc3339(1_582_934_400), "2020-02-29T00:00:00Z");
    }
}
