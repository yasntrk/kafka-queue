'use strict';

const { randomUUID } = require('crypto');
const { publishEvent, disconnect } = require('../producer');

/**
 * End-to-end demo producer.
 *
 * Simulates one full morning auction for the Balıklıova Seafood Cooperative,
 * publishing the real event flow across all services. Uses the sample data from
 * the assignment (Table 1): Levrek, Dil, Kalamar.
 *
 * Run `npm run demo:consume` in another terminal first to watch the events
 * arrive, then run `npm run demo:produce`.
 */
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const now = () => new Date().toISOString();

const sessionId = `session-${now().slice(0, 10)}-${randomUUID().slice(0, 8)}`;

// --- Reference data (from the assignment's Table 1) ---
const captains = [
  { memberId: 'mem-001', memberName: 'Gürkan', boatName: 'Gürkan Kaptan', phone: '+90 555 000 0001' },
  { memberId: 'mem-002', memberName: 'Akın', boatName: 'Akın Kaptan', phone: '+90 555 000 0002' },
  { memberId: 'mem-003', memberName: 'Mehmet', boatName: 'Mehmet Kaptan', phone: '+90 555 000 0003' },
];

const buyers = [
  { buyerId: 'buy-001', name: 'Ayşe Yılmaz', email: 'ayse@example.com', phone: '+90 555 111 1111', address: 'Çeşme, İzmir' },
  { buyerId: 'buy-002', name: 'Deniz Restaurant', email: 'satinalma@deniz.com', phone: '+90 555 222 2222', address: 'Urla, İzmir' },
];

const baskets = [
  { basketId: 'basket-1', species: 'Levrek', quantity: 1.5, unit: 'kg', quality: 'A', basePrice: 90, boatName: 'Gürkan Kaptan' },
  { basketId: 'basket-2', species: 'Dil', quantity: 2, unit: 'kg', quality: 'A', basePrice: 120, boatName: 'Akın Kaptan' },
  { basketId: 'basket-3', species: 'Kalamar', quantity: 2, unit: 'piece', quality: 'B', basePrice: 70, boatName: 'Mehmet Kaptan' },
];

async function publish(topic, payload) {
  await publishEvent(topic, payload);
  console.log(`  -> ${topic}`);
  await sleep(250);
}

async function main() {
  console.log(`\nSimulating auction session: ${sessionId}\n`);

  // 1) Registrations (User & Membership Service)
  console.log('[1] Registrations');
  for (const c of captains) {
    await publish('user.member.registered', { eventId: randomUUID(), ...c, occurredAt: now() });
  }
  for (const b of buyers) {
    await publish('user.buyer.registered', { eventId: randomUUID(), ...b, occurredAt: now() });
  }

  // 2) Catalog (Pre-Auction Catalog Service)
  console.log('[2] Catalog preparation');
  for (const b of baskets) {
    await publish('catalog.basket.created', { eventId: randomUUID(), ...b, occurredAt: now() });
  }
  await publish('catalog.published', {
    eventId: randomUUID(),
    sessionId,
    basketIds: baskets.map((b) => b.basketId),
    totalBaskets: baskets.length,
    occurredAt: now(),
  });

  // 3) Live auction (Auction & Bidding Service)
  console.log('[3] Live auction');
  await publish('auction.session.started', {
    eventId: randomUUID(),
    sessionId,
    startTime: now(),
    totalBaskets: baskets.length,
    occurredAt: now(),
  });

  // basket-1 (Levrek): sold, basket-2 (Dil): sold, basket-3 (Kalamar): unsold this round
  const sold = [];
  for (const b of baskets) {
    await publish('auction.basket.opened', {
      eventId: randomUUID(),
      sessionId,
      basketId: b.basketId,
      basePrice: b.basePrice,
      occurredAt: now(),
    });

    if (b.basketId === 'basket-3') {
      await publish('bid.basket.unsold', {
        eventId: randomUUID(),
        sessionId,
        basketId: b.basketId,
        reason: 'NO_BIDS',
        occurredAt: now(),
      });
      continue;
    }

    const buyer = buyers[sold.length % buyers.length];
    const bidId = `bid-${randomUUID().slice(0, 8)}`;
    const salePrice = b.basePrice + 20;
    await publish('bid.placed', {
      eventId: randomUUID(),
      sessionId,
      basketId: b.basketId,
      buyerId: buyer.buyerId,
      bidId,
      bidAmount: salePrice,
      occurredAt: now(),
    });
    await publish('bid.basket.sold', {
      eventId: randomUUID(),
      sessionId,
      basketId: b.basketId,
      buyerId: buyer.buyerId,
      winningBidId: bidId,
      salePrice,
      occurredAt: now(),
    });
    sold.push({ ...b, buyerId: buyer.buyerId, salePrice });
  }

  // 3b) Re-auction round for the unsold Kalamar basket -> now sold
  console.log('[3b] Re-auction round');
  await publish('bid.rebid.round.opened', {
    eventId: randomUUID(),
    sessionId,
    roundNumber: 2,
    basketIds: ['basket-3'],
    occurredAt: now(),
  });
  {
    const b = baskets.find((x) => x.basketId === 'basket-3');
    const buyer = buyers[0];
    const bidId = `bid-${randomUUID().slice(0, 8)}`;
    const salePrice = 60; // reasonable bid below base, accepted by the cooperative head
    await publish('bid.placed', {
      eventId: randomUUID(),
      sessionId,
      basketId: b.basketId,
      buyerId: buyer.buyerId,
      bidId,
      bidAmount: salePrice,
      occurredAt: now(),
    });
    await publish('bid.basket.sold', {
      eventId: randomUUID(),
      sessionId,
      basketId: b.basketId,
      buyerId: buyer.buyerId,
      winningBidId: bidId,
      salePrice,
      occurredAt: now(),
    });
    sold.push({ ...b, buyerId: buyer.buyerId, salePrice });
  }

  await publish('bid.all.baskets.finalized', {
    eventId: randomUUID(),
    sessionId,
    totalBaskets: baskets.length,
    soldBasketCount: sold.length,
    unsoldBasketCount: baskets.length - sold.length,
    occurredAt: now(),
  });

  // 4) Post-auction & fulfillment
  console.log('[4] Payment & fulfillment');
  for (const s of sold) {
    await publish('bid.payment.confirmed', {
      eventId: randomUUID(),
      sessionId,
      basketId: s.basketId,
      buyerId: s.buyerId,
      amount: s.salePrice,
      paymentStatus: 'CONFIRMED',
      occurredAt: now(),
    });
    await publish('bid.basket.sale.completed', {
      eventId: randomUUID(),
      sessionId,
      basketId: s.basketId,
      buyerId: s.buyerId,
      salePrice: s.salePrice,
      paymentConfirmed: true,
      occurredAt: now(),
    });
    await publish('fulfillment.pickup.scheduled', {
      eventId: randomUUID(),
      sessionId,
      basketId: s.basketId,
      buyerId: s.buyerId,
      method: s.buyerId === 'buy-002' ? 'DELIVERY' : 'PICKUP',
      deliveryAddress: s.buyerId === 'buy-002' ? 'Urla, İzmir' : undefined,
      deliverable: s.buyerId === 'buy-002' ? true : undefined,
      occurredAt: now(),
    });
  }

  // Captain payments (one report per boat, no commission, excluding tax)
  const byBoat = {};
  for (const s of sold) {
    byBoat[s.boatName] = byBoat[s.boatName] || { total: 0, count: 0 };
    byBoat[s.boatName].total += s.salePrice;
    byBoat[s.boatName].count += 1;
  }
  for (const [boatName, agg] of Object.entries(byBoat)) {
    await publish('fulfillment.captain.payment.calculated', {
      eventId: randomUUID(),
      sessionId,
      boatName,
      soldBasketCount: agg.count,
      totalAmount: agg.total,
      occurredAt: now(),
    });
  }

  const totalRevenue = sold.reduce((sum, s) => sum + s.salePrice, 0);
  await publish('auction.closed', {
    eventId: randomUUID(),
    sessionId,
    closedAt: now(),
    totalRevenue,
    soldBasketCount: sold.length,
    unsoldBasketCount: baskets.length - sold.length,
    occurredAt: now(),
  });

  // 5) A couple of notification audit records (Notification Service)
  console.log('[5] Notifications');
  await publish('notification.sent', {
    eventId: randomUUID(),
    channel: 'EMAIL',
    recipient: 'ayse@example.com',
    subject: 'You won basket-1 (Levrek)',
    occurredAt: now(),
  });
  await publish('notification.sent', {
    eventId: randomUUID(),
    channel: 'SMS',
    recipient: 'Gürkan Kaptan',
    subject: 'Captain payment report ready',
    occurredAt: now(),
  });

  console.log(`\nDone. Session ${sessionId}: ${sold.length}/${baskets.length} baskets sold, total revenue ${totalRevenue}.`);
  await disconnect();
}

main().catch(async (err) => {
  console.error('\ndemo:produce failed:', err.message);
  await disconnect().catch(() => {});
  process.exitCode = 1;
});
