export type FilingEvent = {
  eventId: string;
  eventType: "filing.published";
  source: "exchange-filings-simulator";
  occurredAt: string;
  lane: "real-time";
  company: {
    isin: string;
    name: string;
  };
  filing: {
    documentType: string;
    title: string;
    text: string;
  };
};

export const filingsTopic = "filings.realtime";
export const filingsPartitionCount = 3;
