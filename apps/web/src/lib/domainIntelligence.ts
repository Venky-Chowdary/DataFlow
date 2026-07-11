import type { ColumnAnalysis, EnhancedAnalysis } from "./types";

export type DataDomain = "logistics" | "healthcare" | "finance" | "insurance" | "general";

export interface DomainProfile {
  domain: DataDomain;
  label: string;
  confidence: number;
  signals: string[];
  accent: string;
  icon: "truck" | "heart" | "dollar" | "shield" | "database";
  compliance?: string[];
}

const DOMAIN_RULES: {
  id: DataDomain;
  label: string;
  accent: string;
  icon: DomainProfile["icon"];
  patterns: RegExp[];
  compliance?: string[];
  weight: number;
}[] = [
  {
    id: "logistics",
    label: "Logistics & supply chain",
    accent: "#0ea5e9",
    icon: "truck",
    weight: 1,
    patterns: [
      /ship(ping|ment)?/i, /freight/i, /carrier/i, /warehouse/i, /sku/i, /tracking/i,
      /origin/i, /destination/i, /bol/i, /container/i, /pallet/i, /delivery/i,
      /cust_id/i, /order_id/i, /route/i, /manifest/i,
    ],
  },
  {
    id: "healthcare",
    label: "Healthcare & life sciences",
    accent: "#ec4899",
    icon: "heart",
    weight: 1.1,
    compliance: ["HIPAA", "HITECH"],
    patterns: [
      /patient/i, /diagnosis/i, /icd/i, /npi/i, /physician/i, /provider/i,
      /medical/i, /clinical/i, /procedure/i, /rx/i, /prescription/i, /mrn/i,
      /encounter/i, /admission/i, /discharge/i, /lab_/i, /cpt/i,
    ],
  },
  {
    id: "finance",
    label: "Financial services",
    accent: "#8b5cf6",
    icon: "dollar",
    weight: 1.05,
    compliance: ["PCI-DSS", "SOX"],
    patterns: [
      /payment/i, /transaction/i, /ledger/i, /account/i, /balance/i, /currency/i,
      /amt/i, /amount/i, /txn/i, /swift/i, /iban/i, /routing/i, /settlement/i,
      /fee/i, /interest/i, /portfolio/i, /trade/i, /symbol/i,
    ],
  },
  {
    id: "insurance",
    label: "Insurance",
    accent: "#f59e0b",
    icon: "shield",
    weight: 1.05,
    compliance: ["NAIC", "SOC2"],
    patterns: [
      /policy/i, /premium/i, /claim/i, /claimant/i, /underwrit/i, /coverage/i,
      /deductible/i, /beneficiary/i, /insured/i, /loss/i, /adjuster/i, /renewal/i,
    ],
  },
];

export function detectDataDomain(
  columns: string[],
  analysis?: EnhancedAnalysis | null,
): DomainProfile {
  const colText = columns.join(" ").toLowerCase();
  const semanticText = (analysis?.columns ?? [])
    .map((c: ColumnAnalysis) => `${c.column_name} ${c.semantic_type ?? ""}`)
    .join(" ")
    .toLowerCase();

  let best: DomainProfile = {
    domain: "general",
    label: "General enterprise data",
    confidence: 0.55,
    signals: [],
    accent: "#0f766e",
    icon: "database",
  };

  for (const rule of DOMAIN_RULES) {
    const signals: string[] = [];
    for (const col of columns) {
      if (rule.patterns.some((p) => p.test(col))) signals.push(col);
    }
    if (signals.length === 0) {
      for (const p of rule.patterns) {
        if (p.test(colText) || p.test(semanticText)) {
          const m = colText.match(p);
          if (m) signals.push(m[0]);
        }
      }
    }
    const unique = [...new Set(signals)].slice(0, 5);
    const score = Math.min(0.98, (unique.length / Math.max(3, columns.length * 0.15)) * rule.weight);
    if (unique.length >= 2 && score > best.confidence) {
      best = {
        domain: rule.id,
        label: rule.label,
        confidence: score,
        signals: unique,
        accent: rule.accent,
        icon: rule.icon,
        compliance: rule.compliance,
      };
    }
  }

  return best;
}

export function domainFieldGlossary(domain: DataDomain): Record<string, string> {
  const glossaries: Record<DataDomain, Record<string, string>> = {
    logistics: {
      cust_id: "Customer / shipper identifier",
      tracking_no: "Carrier tracking number",
      origin: "Shipment origin facility",
      destination: "Delivery destination",
      weight: "Chargeable weight (kg/lb)",
      freight_class: "NMFC freight classification",
    },
    healthcare: {
      mrn: "Medical record number",
      npi: "National Provider Identifier",
      icd10: "Diagnosis code (ICD-10)",
      dob: "Date of birth (PHI)",
      encounter_id: "Clinical encounter identifier",
    },
    finance: {
      txn_id: "Transaction identifier",
      amt: "Monetary amount",
      ccy: "ISO 4217 currency code",
      account_no: "Account number",
      settlement_date: "Settlement / value date",
    },
    insurance: {
      policy_no: "Policy number",
      premium: "Premium amount",
      claim_id: "Claim identifier",
      coverage_type: "Coverage line of business",
    },
    general: {},
  };
  return glossaries[domain];
}
