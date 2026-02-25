/**
 * GMaps Auto-Order Service
 *
 * Detects newly created GMaps campaigns in the Lead Recycling DB that don't
 * yet have a scrape order in the GMaps dashboard, then auto-creates batches
 * for them based on their configured cities.
 *
 * Runs as a Render Cron Job (free tier) on a schedule.
 */

const { Pool } = require('pg');
const axios = require('axios');

// ── Config ────────────────────────────────────────────────────────────────────

const RECYCLING_DB_URL = process.env.RECYCLING_DB_URL;
const GMAPS_DB_URL     = process.env.GMAPS_DB_URL;
const DASHBOARD_URL    = (process.env.DASHBOARD_URL || 'https://gmaps-dashboard-render.onrender.com').replace(/\/$/, '');
const BATCH_SIZE       = 2000; // Max queries per batch (dashboard splits at this)
const DRY_RUN          = process.env.DRY_RUN === 'true';

// All 211 GMaps business categories (tier 1–4)
const CATEGORIES = [
  // Tier 1 – Universal (22)
  'Accountant','Attorney','Auto repair shop','Bank','Church',
  'Construction company','Dental clinic','Doctor','Funeral home',
  'General contractor','Gym','Hardware store','HVAC contractor',
  'Insurance agency','Law firm','Pharmacy','Place of worship',
  'Real estate agency','School','Tax preparation service','Veterinarian',
  // Tier 2 – Common (46)
  'Appliance store','Architect','Assisted living facility','Auto dealer',
  'Auto parts store','Bowling alley','Car rental agency','Child care agency',
  'Chiropractor','Community center','Credit union','Day care center',
  'Driving school','Electronics store','Employment agency','Eye care center',
  'Financial planner','Fitness center','Furniture store','Golf course',
  'Home goods store','Hospital','Hotel','Martial arts school','Mattress store',
  'Medical clinic','Movie theater','Music school','Nursing home','Optometrist',
  'Personal trainer','Physical therapy clinic','Preschool','Printing service',
  'Property management company','Recreation center','Senior center',
  'Tire shop','Title company','Travel agency','Tutoring service',
  'Urgent care center','Wedding venue','Wellness center','Yoga studio',
  // Tier 3 – Regional (60)
  'Addiction treatment center','After school program','Allergist',
  'Art gallery','Art school','Audiologist','Banquet hall',
  'Building materials store','Business management consultant','Cardiologist',
  'Co-working space','College','Commercial printer','Computer consultant',
  'Computer support and services','Consultant','Country club','Dance school',
  'Department store','Dermatologist','Engineer','Equipment rental agency',
  'Event venue','Gastroenterologist','Gymnastics center','Hospice',
  'Indoor playground','Investment company','Laboratory','Language school',
  'Loan agency','Machine shop','Mental health clinic','Mortgage lender',
  'Motorcycle dealer','Neurologist','Non-profit organization','Notary public',
  'Nursing agency','Nutritionist','Office furniture store','Ophthalmologist',
  'Orthopedic clinic','Osteopath','Pediatrician','Plumbing supply store',
  'Podiatrist','Private investigator','Rehabilitation center','RV dealer',
  'Sports club','Surgeon','Swimming pool','Technical school',
  'Truck dealer','Truck repair shop','University','Urologist',
  'Vocational school','Warehouse',
  // Tier 4 – Metro (83)
  'Aerospace company','Association or organization','Automation company',
  'Biotechnology company','Blood bank','Bottling company','Business school',
  'Call center','Car inspection station','Chamber of commerce','Chemical wholesaler',
  'Computer security service','Computer training school','Conference center',
  'Convention center','Corporate office','Council','Credit counseling service',
  'Culinary school','Data recovery service','Dental laboratory','Diagnostic center',
  'Dialysis center','Distribution service','Education center',
  'Electric motor repair shop','Electrical supply store','Factory',
  'Farm equipment supplier','Fertility clinic','Fitness equipment store',
  'Flight school','Food processing equipment','Freight forwarding service',
  'Geriatrician','Industrial chemicals wholesaler','Industrial design company',
  'Industrial equipment supplier','Insurance company','Labor union',
  'Logistics service','Management school','Manufacturer','Medical diagnostic imaging center',
  'Medical laboratory','Metal fabricator','Metal supplier','MRI center',
  'Office space rental agency','Oncologist','Otolaryngologist','Outlet store',
  'Outpatient surgery center','Packaging supply store','Pain management physician',
  'Payroll service','Pharmaceutical company','Plastic fabrication company',
  'Plastic surgery clinic','Printing equipment supplier','Psychiatric hospital',
  'Radiology center','Reproductive health clinic','Safety equipment supplier',
  'Scientific equipment supplier','Shipping company','Shopping mall','Showroom',
  'Software company','Special education school','Sports complex','Sports medicine clinic',
  'Steel distributor','Steel fabricator','Student housing center','Surgical center',
  'Telecommunications service provider','Tennis court','Textile mill','Tool store',
  'Training centre','Veterinary emergency hospital','Welding supply store',
  'Wholesaler','Women\'s health clinic',
];

// ── DB helpers ────────────────────────────────────────────────────────────────

function makePool(url, ssl = true) {
  return new Pool({
    connectionString: url,
    ssl: ssl ? { rejectUnauthorized: false } : false,
    connectionTimeoutMillis: 15000,
  });
}

// ── Core logic ────────────────────────────────────────────────────────────────

async function run() {
  if (!RECYCLING_DB_URL || !GMAPS_DB_URL) {
    throw new Error('Missing required env vars: RECYCLING_DB_URL, GMAPS_DB_URL');
  }

  const recycling = makePool(RECYCLING_DB_URL, true);
  // Hetzner VPS postgres – no SSL
  const gmaps = makePool(GMAPS_DB_URL, false);

  try {
    // 1. Find all active GMaps campaigns in the recycling DB
    const { rows: campaigns } = await recycling.query(`
      SELECT ic.id, ic.campaign_name
      FROM instant_campaigns ic
      WHERE ic.campaign_name ILIKE '%(GMaps)%'
        AND ic.is_active = true
      ORDER BY ic.id ASC
    `);

    console.log(`[AutoOrder] Found ${campaigns.length} active GMaps campaign(s)`);

    let created = 0;
    let skipped = 0;

    for (const campaign of campaigns) {
      console.log(`\n[AutoOrder] Checking: "${campaign.campaign_name}" (ID ${campaign.id})`);

      // 2. Check if any batch already exists for this campaign.
      //    Check by lead_recycling_campaign_id first, then fall back to campaign name match
      //    (older batches may not have lead_recycling_campaign_id set)
      const { rows: existing } = await gmaps.query(`
        SELECT COUNT(*)::int AS cnt
        FROM jobs_batch
        WHERE lead_recycling_campaign_id = $1
           OR campaign_name = $2
           OR name LIKE $3
      `, [campaign.id, campaign.campaign_name, `${campaign.campaign_name}%`]);

      if (existing[0].cnt > 0) {
        console.log(`  ↳ Skipping – already has ${existing[0].cnt} batch(es)`);
        skipped++;
        continue;
      }

      // 3. Get configured cities – try campaign_cities first, fall back to client_city_claims
      //    (older campaigns only have client_city_claims; newer ones have both)
      const { rows: allCities } = await recycling.query(`
        SELECT DISTINCT c.city_name, c.state_code
        FROM (
          -- campaign_cities (pipeline integration table)
          SELECT cc.city_id
          FROM campaign_cities cc
          WHERE cc.campaign_id = $1
          UNION
          -- client_city_claims (original city ownership table)
          SELECT ccc.city_id
          FROM client_city_claims ccc
          JOIN instant_campaigns ic ON ic.client_id = ccc.client_id
          WHERE ic.id = $1
        ) combined
        JOIN cities c ON c.id = combined.city_id
        WHERE c.city_name NOT ILIKE '%county%'   -- skip county-level entries
        ORDER BY c.city_name
      `, [campaign.id]);

      if (allCities.length === 0) {
        console.log(`  ↳ Skipping – no cities configured`);
        skipped++;
        continue;
      }

      console.log(`  ↳ ${allCities.length} city/cities: ${allCities.map(c => `${c.city_name}, ${c.state_code}`).join(' | ')}`);

      // 4. Build query list (category × city)
      const queries = [];
      const targetStates = new Set();

      for (const city of allCities) {
        const location = `${city.city_name}, ${city.state_code}`;
        for (const cat of CATEGORIES) {
          queries.push(`${cat} in ${location}`);
        }
        targetStates.add(city.state_code);
      }

      const totalParts = Math.ceil(queries.length / BATCH_SIZE);
      console.log(`  ↳ ${queries.length} queries → ${totalParts} batch part(s) | states: ${[...targetStates].join(',')}`);

      if (DRY_RUN) {
        console.log(`  ↳ DRY RUN – skipping batch creation`);
        continue;
      }

      // 5. Create one or more batches (split at BATCH_SIZE)
      //    campaign_name = the base name (no "Part N") so Instantly matching works
      const baseName    = campaign.campaign_name;           // e.g. "Spotless Group (GMaps)"
      const statesArray = [...targetStates];

      for (let i = 0; i < totalParts; i++) {
        const partNum   = i + 1;
        const batchName = totalParts > 1 ? `${baseName} Part ${partNum}` : baseName;
        const chunk     = queries.slice(i * BATCH_SIZE, (i + 1) * BATCH_SIZE);

        const payload = {
          name: batchName,
          queries: chunk,
          autoImport: true,
          targetStates: statesArray,
          leadRecyclingCampaignId: campaign.id,
        };

        const { data } = await axios.post(`${DASHBOARD_URL}/api/batches`, payload, {
          timeout: 30000,
        });

        // Fix: campaign_name must NOT include "Part N" so the pipeline finds
        // the correct Instantly campaign when auto-importing
        if (totalParts > 1) {
          await gmaps.query(
            `UPDATE jobs_batch SET campaign_name = $1 WHERE batch_id = $2`,
            [baseName, data.batchId]
          );
        }

        console.log(`  ↳ Created "${batchName}" → batchId ${data.batchId} (${chunk.length} queries)`);
        created++;

        // Small delay between parts to avoid hammering the API
        if (i < totalParts - 1) {
          await new Promise(r => setTimeout(r, 1000));
        }
      }
    }

    console.log(`\n[AutoOrder] Done. Created ${created} batch(es), skipped ${skipped} campaign(s).`);
  } finally {
    await recycling.end();
    await gmaps.end();
  }
}

// ── Entry point ───────────────────────────────────────────────────────────────

run()
  .then(() => process.exit(0))
  .catch(err => {
    console.error('[AutoOrder] FATAL:', err.message);
    process.exit(1);
  });
