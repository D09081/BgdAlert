#!/usr/bin/env bash
# ============================================================================
# Подключение своего домена к уже установленному серверу (install.sh) +
# бесплатный HTTPS-сертификат Let's Encrypt.
#
# Запускать НА ТОМ ЖЕ VPS, ПОСЛЕ install.sh, от пользователя с sudo:
#   bash install-domain.sh
#
# Зачем это вообще нужно: без HTTPS браузеры не разрешают push-уведомления —
# это ограничение самих браузеров, не проекта (см. install.sh). Свой домен +
# этот скрипт снимают это ограничение и заодно избавляют от порта :3000
# в адресной строке.
# ============================================================================
set -euo pipefail

SERVICE_NAME="belgorod-alert"
PORT="3000"
SERVER_IP="$(curl -fsSL ifconfig.me || hostname -I | awk '{print $1}')"

echo "==> 0/5 Проверка, что сервер (${SERVICE_NAME}) вообще установлен и запущен..."
if ! systemctl is-active --quiet "$SERVICE_NAME" 2>/dev/null; then
  echo "    Сервис '$SERVICE_NAME' не найден или не запущен."
  echo "    Сначала выполни install.sh, потом уже этот скрипт."
  exit 1
fi

read -rp "Домен (например bgdalert.online, БЕЗ www и БЕЗ https://): " DOMAIN
if [ -z "$DOMAIN" ]; then echo "Домен не может быть пустым"; exit 1; fi

echo ""
echo "==> ВАЖНО: перед продолжением зайди в панель управления доменом"
echo "    (там, где покупал — AdminVPS и т.п.) и создай ДВЕ A-записи,"
echo "    указывающие на IP этого сервера:"
echo ""
echo "      $DOMAIN        A     $SERVER_IP"
echo "      www.$DOMAIN    A     $SERVER_IP"
echo ""
echo "    DNS обновляется от нескольких минут до нескольких часов."
echo "    Проверить самому можно так: dig +short $DOMAIN"
echo ""
read -rp "DNS уже настроен и применился? (y/N): " CONFIRM
if [ "$CONFIRM" != "y" ] && [ "$CONFIRM" != "Y" ]; then
  echo "Ок — настрой A-записи и запусти этот скрипт снова, когда DNS применится."
  exit 0
fi

read -rp "Email для уведомлений Let's Encrypt (Enter — пропустить, без email): " EMAIL
CERTBOT_EMAIL_ARGS="--register-unsafely-without-email"
if [ -n "$EMAIL" ]; then
  CERTBOT_EMAIL_ARGS="-m $EMAIL --no-eff-email"
fi

echo "==> 1/5 Устанавливаю nginx и certbot (если ещё не стоят)..."
sudo apt-get update -y
sudo apt-get install -y nginx certbot python3-certbot-nginx

echo "==> 2/5 Настраиваю nginx как обратный прокси на 127.0.0.1:$PORT для $DOMAIN..."
sudo tee "/etc/nginx/sites-available/$DOMAIN" > /dev/null << EOF
server {
    listen 80;
    server_name $DOMAIN www.$DOMAIN;

    location / {
        proxy_pass http://127.0.0.1:$PORT;
        proxy_http_version 1.1;
        proxy_set_header Upgrade \$http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
    }
}
EOF
sudo ln -sf "/etc/nginx/sites-available/$DOMAIN" "/etc/nginx/sites-enabled/$DOMAIN"
sudo rm -f /etc/nginx/sites-enabled/default
sudo nginx -t
sudo systemctl reload nginx

echo "==> 3/5 Открываю порты 80/443 в файрволе (если ufw установлен)..."
if command -v ufw >/dev/null 2>&1; then
  sudo ufw allow 'Nginx Full' || true
fi

echo "==> 4/5 Получаю бесплатный HTTPS-сертификат Let's Encrypt для $DOMAIN..."
sudo certbot --nginx -d "$DOMAIN" -d "www.$DOMAIN" --redirect --non-interactive --agree-tos $CERTBOT_EMAIL_ARGS

echo "==> 5/5 Проверяю автопродление сертификата (certbot ставит его сам)..."
sudo systemctl list-timers | grep -i certbot || true

echo ""
echo "============================================================"
echo " Готово! Сайт теперь доступен по адресу:"
echo "   https://$DOMAIN"
echo "   https://$DOMAIN/admin.html"
echo ""
echo " HTTPS снимает ограничение на push-уведомления в браузере — раньше"
echo " без домена это было недоступно, теперь заработает само, донастраивать"
echo " ничего не нужно."
echo ""
echo " Порт :$PORT напрямую по IP ($SERVER_IP:$PORT) пока продолжает работать"
echo " параллельно (nginx его не отключает) — если хочешь, чтобы сайт был"
echo " доступен ТОЛЬКО через домен и https, скажи, добавлю отдельным шагом"
echo " закрытие порта $PORT наружу в файрволе."
echo ""
echo " Сертификат Let's Encrypt обновляется автоматически (systemd-таймер"
echo " certbot делает это сам, см. вывод выше) — раз в ~90 дней, без участия."
echo "============================================================"
