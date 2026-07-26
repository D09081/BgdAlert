#!/usr/bin/env bash
# ============================================================================
# Установочный скрипт «Тревога Белгород» — без домена, без HTTPS.
# Запускать на самом VPS от пользователя с sudo-правами:
#   bash install.sh
#
# ВАЖНО про push-уведомления в браузере: без HTTPS браузеры их просто не
# разрешат (это требование самих браузеров, не проекта). Сайт, лента,
# админка и Telegram-бот при этом будут работать полностью — ограничение
# касается только push-уведомлений через сам сайт. Если понадобится HTTPS
# позже — есть отдельная инструкция с бесплатным поддоменом DuckDNS.
# ============================================================================
set -euo pipefail

REPO_URL="https://github.com/D09081/BgdAlert.git"
INSTALL_DIR="$HOME/BgdAlert"
SERVICE_NAME="belgorod-alert"
PORT="3000"

echo "==> 1/6 Устанавливаю Node.js 20, git, unzip (если ещё не стоят)..."
if ! command -v node >/dev/null 2>&1 || [ "$(node -v | cut -d. -f1 | tr -d v)" -lt 18 ]; then
  curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
  sudo apt-get install -y nodejs
fi
sudo apt-get install -y git unzip

echo "==> 2/6 Клонирую/обновляю репозиторий в $INSTALL_DIR..."
if [ -d "$INSTALL_DIR/.git" ]; then
  git -C "$INSTALL_DIR" pull
else
  git clone "$REPO_URL" "$INSTALL_DIR"
fi

echo "==> 3/6 Ставлю зависимости (npm install)..."
cd "$INSTALL_DIR"
npm install

echo "==> 4/6 Настройка переменных окружения."
echo "    Пароль администратора — оставь пустым, чтобы сгенерировался автоматически"
echo "    (он сохранится на диске между перезапусками, посмотришь в логах: sudo journalctl -u $SERVICE_NAME -f)"
read -rp "    ADMIN_PASSWORD (Enter — авто): " ADMIN_PASSWORD
read -rp "    TELEGRAM_BOT_TOKEN (Enter — пропустить, бот будет выключен): " TELEGRAM_BOT_TOKEN
TELEGRAM_BOT_USERNAME=""
if [ -n "$TELEGRAM_BOT_TOKEN" ]; then
  read -rp "    TELEGRAM_BOT_USERNAME (без @): " TELEGRAM_BOT_USERNAME
fi

ENV_LINES="Environment=PORT=$PORT"
[ -n "$ADMIN_PASSWORD" ] && ENV_LINES="$ENV_LINES
Environment=ADMIN_PASSWORD=$ADMIN_PASSWORD"
[ -n "$TELEGRAM_BOT_TOKEN" ] && ENV_LINES="$ENV_LINES
Environment=TELEGRAM_BOT_TOKEN=$TELEGRAM_BOT_TOKEN"
[ -n "$TELEGRAM_BOT_USERNAME" ] && ENV_LINES="$ENV_LINES
Environment=TELEGRAM_BOT_USERNAME=$TELEGRAM_BOT_USERNAME"

echo "==> 5/6 Создаю systemd-сервис (работает постоянно, переживает перезагрузку и отключение SSH)..."
sudo tee "/etc/systemd/system/${SERVICE_NAME}.service" > /dev/null << EOF
[Unit]
Description=Тревога Белгород
After=network.target

[Service]
Type=simple
User=$USER
WorkingDirectory=$INSTALL_DIR
ExecStart=$(command -v node) server.js
Restart=always
RestartSec=5
$ENV_LINES

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable "$SERVICE_NAME"
sudo systemctl restart "$SERVICE_NAME"

echo "==> 6/6 Открываю порт $PORT в файрволе (если ufw установлен)..."
if command -v ufw >/dev/null 2>&1; then
  sudo ufw allow "$PORT"/tcp || true
  sudo ufw allow OpenSSH || true
fi

SERVER_IP="$(curl -fsSL ifconfig.me || hostname -I | awk '{print $1}')"

echo ""
echo "============================================================"
echo " Готово! Проверка статуса:"
echo "   sudo systemctl status $SERVICE_NAME"
echo " Логи в реальном времени (там же будет ADMIN_PASSWORD, если оставил пустым):"
echo "   sudo journalctl -u $SERVICE_NAME -f"
echo ""
echo " Сайт:   http://$SERVER_IP:$PORT"
echo " Админка: http://$SERVER_IP:$PORT/admin.html"
echo "============================================================"
echo ""
echo "Чтобы обновить проект в будущем после изменений на GitHub:"
echo "  cd $INSTALL_DIR && git pull && npm install && sudo systemctl restart $SERVICE_NAME"
