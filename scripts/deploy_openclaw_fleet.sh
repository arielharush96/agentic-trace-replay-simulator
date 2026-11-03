#!/usr/bin/env bash
set -euo pipefail

NS="${NS:-trace-replay}"
COUNT="${COUNT:-10}"
TARGET_NODE="${TARGET_NODE:?set TARGET_NODE to a schedulable worker node}"

PATCH_SCRIPT=$(cat <<'PATCH_SCRIPT_EOF'
node -e 'const fs=require("fs"); const p="/opt/openshell/bin/openshell-fleet"; fs.writeFileSync(p, "#!/usr/bin/env node\nconst {spawnSync}=require(\"child_process\");\nconst args=process.argv.slice(2);\nconst workspace=process.env.OPENSHELL_FLEET_WORKSPACE || \"default\";\nconst targetNode=process.env.OPENSHELL_FLEET_NODE || \"\";\nif (!args.includes(\"--workspace\")) args.unshift(\"--workspace\",workspace);\nif (args[2] === \"sandbox\" && args[3] === \"create\") { const marker=args.indexOf(\"--\"); const options=[\"--driver-config-json\", JSON.stringify({kubernetes:{pod:{node_selector:{\"kubernetes.io/hostname\":targetNode}}}})]; args.splice(marker < 0 ? args.length : marker, 0, ...options); }\nconst result=spawnSync(\"/opt/openshell/bin/openshell\", args, {stdio:\"inherit\"}); process.exit(result.status ?? 1);\n"); fs.chmodSync(p,0o755);'
PATCH_SCRIPT_EOF
)

for index in $(seq 0 $((COUNT - 1))); do
  name="openclaw-fleet-$index"
  app="openclaw-fleet-$index"
  config="openclaw-shell-fleet-config-$index"
  oc -n "$NS" get configmap openclaw-shell-config -o json \
    | jq --arg command "/opt/openshell/bin/openshell-fleet" --arg workspace "fleet$index" --arg config "$config" '
        del(.metadata.uid,.metadata.resourceVersion,.metadata.creationTimestamp,.metadata.managedFields)
        | .metadata.name=$config
        | .data["openclaw.json"]=(.data["openclaw.json"] | fromjson | .plugins.entries.openshell.config.command=$command | .plugins.entries.openshell.config.workspace=$workspace | tojson)
      ' | oc apply -f -
  oc -n "$NS" delete deployment "$name" service "$name" --ignore-not-found >/dev/null 2>&1 || true
  oc -n "$NS" get deployment openclaw-shell -o json \
    | jq --arg name "$name" --arg app "$app" --arg node "$TARGET_NODE" --arg patch "$PATCH_SCRIPT" --arg config "$config" '
      del(.metadata.uid,.metadata.resourceVersion,.metadata.generation,.metadata.creationTimestamp,.metadata.managedFields,.status)
      | .metadata.name=$name
      | .metadata.labels.app=$app
      | .spec.replicas=1
      | .spec.selector.matchLabels.app=$app
      | .spec.template.metadata.labels.app=$app
      | .spec.template.spec.nodeSelector={"kubernetes.io/hostname":$node}
      | .spec.template.spec.volumes |= map(if .name=="openclaw-shell-home" then {name:.name,emptyDir:{}} elif .name=="config-template" then . + {configMap:{name:$config}} else . end)
      | .spec.template.spec.containers |= map(if .name=="gateway" then .resources={requests:{cpu:"1",memory:"2Gi"},limits:{cpu:"2",memory:"4Gi"}} | .env += [{name:"OPENSHELL_FLEET_WORKSPACE",value:("fleet" + ($name | split("-")[-1]))},{name:"OPENSHELL_FLEET_NODE",value:$node}] else . end)
      | .spec.template.spec.initContainers |= map(if .name=="init-config" then .args[0]=(.args[0]+"\n"+$patch) else . end)
    ' | oc apply -f -
  cat <<EOF | oc -n "$NS" apply -f -
apiVersion: v1
kind: Service
metadata:
  name: $name
  labels:
    app: $app
spec:
  clusterIP: None
  selector:
    app: $app
  ports:
  - name: gateway
    port: 18790
    targetPort: 18790
EOF
done

for index in $(seq 0 $((COUNT - 1))); do
  oc -n "$NS" rollout status deployment/openclaw-fleet-$index --timeout=600s
done
