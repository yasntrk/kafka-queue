#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Fish Auction — fire every notification type, for testing the Notification UI.
# Usage:  ./notif-test.sh [HOST]        (HOST default = EC2 public IP)
# Tip:    watch them on the Notification dashboard, or log in as a MEMBER
#         (a BUYER only sees bid-started/ended notifications by design).
# ---------------------------------------------------------------------------
HOST="${1:-18.203.33.170}"
U="http://$HOST:3001"; C="http://$HOST:3002"; F="http://$HOST:3004"
echo "== Notification test -> $HOST =="

echo "[1] Member Registered  (user.member.registered)"
curl -s -X POST "$U/members/register" -H "Content-Type: application/json" \
  -d '{"memberName":"Notif Kaptan","boatName":"Notif Tekne","email":"nk@balik.tr","password":"test1234"}' >/dev/null && echo "    ok"

echo "[2] Buyer Registered   (user.buyer.registered)"
curl -s -X POST "$U/buyers/register" -H "Content-Type: application/json" \
  -d '{"name":"Notif Alici","email":"na@balik.tr","address":"Izmir","password":"test1234"}' >/dev/null && echo "    ok"

echo "[3] Catalog Published  (catalog.published)"
BID=$(curl -s -X POST "$C/api/baskets" -H "Content-Type: application/json" \
  -d '{"species":"Notif Levrek","quantity":40,"unit":"kg","quality":"A","boatName":"Notif Tekne"}' \
  | python3 -c 'import sys,json;print(json.load(sys.stdin).get("basket_id",""))')
curl -s -X PATCH "$C/api/baskets/$BID/price" -H "Content-Type: application/json" -d '{"basePrice":35}' >/dev/null
curl -s -X POST "$C/api/catalog/publish" -H "Content-Type: application/json" -d '{"title":"Notif Test Mezati"}' >/dev/null && echo "    ok (basket=$BID)"

echo "[4] Sale + Pickup + Captain Payment"
echo "    (bid.basket.sold, fulfillment.pickup.scheduled, fulfillment.captain.payment.calculated)"
echo "    -> these are Kafka-internal; firing them via the seeder:"
( cd "$(dirname "$0")" && SEED_HOST="$HOST" node --env-file-if-exists=.env seed-demo.js 2>/dev/null \
  | grep -iE "SATILDI|pickup|TAMAMLAND|kaptan" ) && echo "    ok"

echo "== Done =="
