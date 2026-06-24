# Karpenter's ephemeral-storage allocatable estimate reads the
# blockDeviceMapping flagged `rootVolume: true` (falling back to the
# AMI's RootDeviceName, /dev/xvda on the rio NixOS images). The AMI's
# rio-kubelet-mount mounts /dev/xvdb at /var/lib/kubelet, and
# controller.toml's `max_node_disk` is derived from `quotaVolumeSize`
# (xvdb), so without the flag Karpenter sees only the 40Gi xvda and
# resolves ZERO candidate instance types for any controller-minted
# NodeClaim whose `requests.ephemeral-storage` exceeds ~35Gi —
# `failed launching nodeclaim: all requested instance types were
# unavailable during launch` with NO aws-error-code, then the claim is
# GC'd and rio-controller ICE-masks the cell. 3e1472343 shrank xvda
# 500Gi→40Gi without adding the flag; the bug is latent until backlog
# pushes per-claim ephemeral past ~35Gi.
#
# rio-nvme stays exempt: instanceStorePolicy:RAID0 already credits
# instance-store toward ephemeral (karpenter.yaml's rio-nvme block),
# and xvdb is not mapped there (48-quota-volume-pin.sh).
#
# (documentary — .sh is not tracey-scanned.)

. "$(dirname "$0")/_lib.sh"

T=$TMPDIR/ephemeral-rootvolume
rm -rf "$T"; mkdir -p "$T"

render_karpenter -s templates/karpenter.yaml > "$T/render.yaml"

# Every EBS-only NodeClass: the xvdb mapping carries rootVolume:true,
# and it is the ONLY mapping that does (the CRD allows at most one).
for nc in rio-default rio-default-express rio-metal rio-fetcher; do
  yq -N "select(.kind==\"EC2NodeClass\" and .metadata.name==\"$nc\")" \
     "$T/render.yaml" > "$T/$nc.yaml"

  rv=$(yq -N '.spec.blockDeviceMappings[] | select(.rootVolume==true) | .deviceName' "$T/$nc.yaml")
  if [ "$rv" != "/dev/xvdb" ]; then
    echo "FAIL: EC2NodeClass $nc — rootVolume:true is on '${rv:-<none>}', want /dev/xvdb." >&2
    echo "      Karpenter's ephemeral-storage estimate falls back to the AMI" >&2
    echo "      RootDeviceName (xvda, $(yq -r '.karpenter.rootVolumeSize' values.yaml));" >&2
    echo "      controller NodeClaims requesting >~35Gi resolve to zero candidate types" >&2
    echo "      and the cell ICE-masks. controller.toml max_node_disk is derived from" >&2
    echo "      quotaVolumeSize (xvdb) — that contract requires Karpenter see xvdb too." >&2
    exit 1
  fi

  n=$(yq -N '[.spec.blockDeviceMappings[] | select(.rootVolume==true)] | length' "$T/$nc.yaml")
  if [ "$n" != "1" ]; then
    echo "FAIL: EC2NodeClass $nc has $n rootVolume:true mappings; CRD allows at most one" >&2
    exit 1
  fi
done

# rio-nvme: no xvdb (48-quota-volume-pin.sh), so no rootVolume flag —
# ephemeral comes from instanceStorePolicy:RAID0.
yq -N 'select(.kind=="EC2NodeClass" and .metadata.name=="rio-nvme")' \
   "$T/render.yaml" > "$T/rio-nvme.yaml"
if yq -e '.spec.blockDeviceMappings[] | select(.rootVolume==true)' "$T/rio-nvme.yaml" >/dev/null 2>&1; then
  echo "FAIL: rio-nvme carries rootVolume:true on an EBS mapping; instance-store" >&2
  echo "      RAID0 owns its kubelet root via instanceStorePolicy — flagging xvda" >&2
  echo "      would mis-account ephemeral on nvme classes" >&2
  exit 1
fi

echo "ephemeral rootVolume: rio-default{,-express}+rio-metal+rio-fetcher flag /dev/xvdb; rio-nvme exempt (instanceStorePolicy)"
