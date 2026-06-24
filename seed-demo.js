/* =====================================================================
 * Demo data seeder for the Fish Auction system.
 * Drives the REAL service APIs end-to-end and produces the bid.basket.sold
 * events directly to Kafka (the catalog->bidding contract gap means bidding
 * can't open/sell baskets from catalog.published alone).
 *
 * Usage (from kafka-queue/):
 *   SEED_HOST=18.203.33.170 node --env-file-if-exists=.env seed-demo.js
 * Defaults SEED_HOST to localhost if unset.
 * ===================================================================== */
'use strict';
const { randomUUID } = require('crypto');
const kafka = require('./src/kafka');

const HOST = process.env.SEED_HOST || 'localhost';
const U = `http://${HOST}:3001`; // user & membership
const C = `http://${HOST}:3002`; // catalog
const F = `http://${HOST}:3004`; // post-auction (fulfillment)
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const now = () => new Date().toISOString();

async function api(method, url, body) {
  const res = await fetch(url, {
    method,
    headers: { 'Content-Type': 'application/json' },
    body: body ? JSON.stringify(body) : undefined,
  });
  let data = null;
  try { data = await res.json(); } catch { /* some endpoints redirect/empty */ }
  return { ok: res.ok, status: res.status, data };
}

const MEMBERS = [
  { memberName: 'Ali Reis', boatName: 'Karadeniz Yıldızı', email: 'ali@reis.tr', phone: '0532 111 1111', password: 'demo1234' },
  { memberName: 'Veli Kaptan', boatName: 'Ege İncisi', email: 'veli@kaptan.tr', phone: '0532 222 2222', password: 'demo1234' },
];
const BUYERS = [
  { name: 'Mehmet Balıkçılık', email: 'mehmet@balik.tr', phone: '0533 333 3333', address: 'Kemeraltı Çarşısı No:12, Konak, İzmir', password: 'demo1234' },
  { name: 'Deniz Su Ürünleri', email: 'info@denizsu.tr', phone: '0533 444 4444', address: 'Sahil Yolu No:48, Karşıyaka, İzmir', password: 'demo1234' },
  { name: 'Marmara Restoran', email: 'satinalma@marmara.tr', phone: '0533 555 5555', address: 'Liman Cad. No:3, Çeşme, İzmir', password: 'demo1234' },
];
const BASKETS = [
  { species: 'Çipura', quantity: 80, quality: 'A', boatName: 'Karadeniz Yıldızı', basePrice: 50, salePrice: 64 },
  { species: 'Levrek', quantity: 120, quality: 'A', boatName: 'Ege İncisi', basePrice: 45, salePrice: 58 },
  { species: 'Hamsi', quantity: 200, quality: 'B', boatName: 'Karadeniz Yıldızı', basePrice: 18, salePrice: 25 },
  { species: 'Mezgit', quantity: 60, quality: 'B', boatName: 'Ege İncisi', basePrice: 30, salePrice: 41 },
];

async function main() {
  console.log(`\n=== Seeding demo data → ${HOST} ===\n`);

  // 1) Members (captains)
  console.log('1) Kaptanlar (üye) kaydı...');
  for (const m of MEMBERS) {
    const r = await api('POST', `${U}/members/register`, m);
    console.log(`   ${r.ok ? '+' : 'x'} ${m.memberName} (${m.boatName})${r.ok ? ' → ' + r.data.memberId : ' :: ' + JSON.stringify(r.data)}`);
  }

  // 1b) Map boatName -> memberId (so baskets link to a captain → captain payouts)
  const memberByBoat = {};
  const ml = await api('GET', `${U}/api/members`);
  if (ml.ok && Array.isArray(ml.data)) {
    for (const m of ml.data) memberByBoat[m.boat_name || m.boatName] = m.member_id || m.memberId;
  }
  console.log('   boat→memberId:', JSON.stringify(memberByBoat));

  // 2) Buyers
  console.log('2) Alıcı kaydı...');
  const buyerIds = [];
  for (const b of BUYERS) {
    const r = await api('POST', `${U}/buyers/register`, b);
    if (r.ok && r.data.buyerId) buyerIds.push(r.data.buyerId);
    console.log(`   ${r.ok ? '+' : 'x'} ${b.name}${r.ok ? ' → ' + r.data.buyerId : ' :: ' + JSON.stringify(r.data)}`);
  }
  if (buyerIds.length === 0) buyerIds.push('BUY-DEMO01');

  // 3) Catalog: baskets + prices
  console.log('3) Katalog: sepet oluştur + fiyatla...');
  const baskets = [];
  for (const bk of BASKETS) {
    const r = await api('POST', `${C}/api/baskets`, {
      species: bk.species, quantity: bk.quantity, unit: 'kg', quality: bk.quality, boatName: bk.boatName,
      memberId: memberByBoat[bk.boatName] || undefined,
    });
    if (!r.ok) { console.log(`   x sepet ${bk.species} :: ${JSON.stringify(r.data)}`); continue; }
    const basketId = r.data.basket_id || r.data.basketId;
    const pr = await api('PATCH', `${C}/api/baskets/${basketId}/price`, { basePrice: bk.basePrice });
    baskets.push({ ...bk, basketId });
    console.log(`   + ${bk.species} ${bk.quantity}kg (${bk.quality}) → ${basketId}  fiyat=${pr.ok ? bk.basePrice : 'HATA'}`);
  }

  // 4) Publish catalog → session
  console.log('4) Katalog yayınla...');
  const pub = await api('POST', `${C}/api/catalog/publish`, { title: `Sabah Mezatı — ${new Date().toLocaleDateString('tr-TR')}` });
  if (!pub.ok) { console.log(`   x publish :: ${JSON.stringify(pub.data)}`); }
  const sessionId = pub.data && (pub.data.sessionId || pub.data.session_id);
  console.log(`   + session = ${sessionId}  (${pub.data && pub.data.basketIds ? pub.data.basketIds.length : 0} sepet)`);
  console.log('   (catalog.published → bidding oturumu + post-auction projeksiyonu dolar)');

  await sleep(4000); // let basket.created + published propagate to consumers

  // 5) Produce bid.basket.sold directly to Kafka (one per basket)
  console.log('5) Satış event\'leri üret (bid.basket.sold → Confluent)...');
  const producer = kafka.producer();
  await producer.connect();
  let i = 0;
  for (const bk of baskets) {
    const buyerId = buyerIds[i % buyerIds.length]; i++;
    const payload = {
      eventName: 'bid.basket.sold',
      eventId: randomUUID(),
      sessionId,
      basketId: bk.basketId,
      buyerId,
      winningBidId: randomUUID(),
      salePrice: bk.salePrice,
      occurredAt: now(),
    };
    await producer.send({ topic: 'bid.basket.sold', messages: [{ key: sessionId, value: JSON.stringify(payload) }] });
    console.log(`   + SATILDI ${bk.species} → ${buyerId} @ ${bk.salePrice}₺  (basket ${bk.basketId})`);
  }
  await producer.disconnect();

  await sleep(7000); // let post-auction record the sales

  // 6) Post-auction: schedule pickup + complete some, calculate captain payments
  console.log('6) Satış sonrası akış (pickup / tamamla / kaptan ödemesi)...');
  for (let k = 0; k < baskets.length; k++) {
    const bk = baskets[k];
    const pk = await api('POST', `${F}/fulfillment/sales/${bk.basketId}/pickup`, {
      pickupLocation: 'Balıklıova Kooperatif Soğuk Hava Deposu',
      pickupTimeWindow: '08:00–10:00',
    });
    let done = '';
    if (k < 2) { // complete first two → "Completed" stat
      const cp = await api('POST', `${F}/fulfillment/sales/${bk.basketId}/complete`);
      done = cp.ok ? ' + TAMAMLANDI' : ` (complete: ${cp.status})`;
    }
    console.log(`   ${pk.ok ? '+' : 'x'} pickup ${bk.species}${done}`);
  }
  if (sessionId) {
    const cap = await api('POST', `${F}/fulfillment/sessions/${sessionId}/captain-payments/calculate`);
    console.log(`   ${cap.ok ? '+' : 'x'} kaptan ödemeleri hesaplandı (session ${sessionId})`);
  }

  console.log('\n=== Seed tamam. Dashboard\'ları yenile. ===\n');
}

main().then(() => process.exit(0)).catch((e) => { console.error('SEED ERROR:', e); process.exit(1); });
