#!/bin/sh
# 安装 oc-orch CLI 到 PATH，并可选安装 systemd user service。
# 用法：sh deploy/install-user-service.sh [--with-systemd]
set -eu

REPO="$(cd "$(dirname "$0")/.." && pwd)"
HOME_DIR="${OC_ORCH_HOME:-$HOME/.local/share/opencode-orchestrator}"

mkdir -p "$HOME/.local/bin" "$HOME_DIR"
ln -sfn "$REPO/bin/oc-orch" "$HOME/.local/bin/oc-orch"
chmod 700 "$HOME_DIR"

"$HOME/.local/bin/oc-orch" --home "$HOME_DIR" init
"$HOME/.local/bin/oc-orch" --home "$HOME_DIR" doctor

# Render only machine-specific paths at install time.
install_unit() {
  python3 - "$REPO" "$1" <<'PYTHON'
import os
from pathlib import Path
import shutil
import sys

repo, unit = Path(sys.argv[1]), sys.argv[2]
template = (repo / "deploy" / unit).read_text()
if unit == "oc-orch-email.service":
    paths = [str(Path.home() / ".local/bin")]
    for command in ("agently-cli", "node"):
        binary = shutil.which(command)
        if binary:
            paths.append(os.path.dirname(os.path.abspath(binary)))
    paths.extend(["/usr/local/bin", "/usr/bin", "/bin"])
    value = os.pathsep.join(dict.fromkeys(paths))
    # Escape a quoted Environment= assignment and systemd's percent specifiers.
    value = value.replace("\\", "\\\\").replace('"', '\\"').replace("%", "%%")
    template = template.replace("@MAIL_PATH@", value)
else:
    template = template.replace("@OC_ORCH_REPO@", str(repo).replace("%", "%%"))
dest = Path.home() / ".config/systemd/user" / unit
dest.write_text(template)
PYTHON
}

if [ "${1:-}" = "--with-systemd" ]; then
  mkdir -p "$HOME/.config/systemd/user"
  install_unit oc-orch-supervisor.service
  systemctl --user daemon-reload
  systemctl --user enable --now oc-orch-supervisor
  systemctl --user status oc-orch-supervisor --no-pager || true
else
  "$HOME/.local/bin/oc-orch" --home "$HOME_DIR" supervisor start
fi

if [ "${1:-}" = "--with-email" ] || [ "${2:-}" = "--with-email" ]; then
  mkdir -p "$HOME/.config/systemd/user"
  install_unit oc-orch-email.service
  cp "$REPO/deploy/oc-orch-email.timer" \
    "$HOME/.config/systemd/user/oc-orch-email.timer"
  systemctl --user daemon-reload
  systemctl --user enable --now oc-orch-email.timer
  systemctl --user list-timers oc-orch-email.timer --no-pager || true
fi

echo "installed: $HOME/.local/bin/oc-orch"
echo "state:     $HOME_DIR"
