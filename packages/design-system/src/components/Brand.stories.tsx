import type { Meta, StoryObj } from "@storybook/react";
import { AppShell } from "./AppShell";
import { BrandLogo } from "./BrandLogo";
import { PageHeader } from "./PageHeader";
import { SelectOptionCard } from "./SelectOptionCard";
import { Button } from "./Button";
import { useState } from "react";

const STEPS = [
  { id: "source", label: "Source", description: "File, database, or API" },
  { id: "destination", label: "Destination", description: "Warehouse or export" },
  { id: "transfer", label: "Transfer", description: "Preflight and execute" },
];

const meta: Meta = {
  title: "Brand/Enterprise Shell",
  parameters: { layout: "fullscreen" },
};

export default meta;

export const FullShell: StoryObj = {
  render: () => (
    <AppShell steps={STEPS} currentStepIndex={0}>
      <PageHeader
        title="Configure source"
        subtitle="Enterprise-grade data operations with AI-assisted schema inference and fail-fast preflight."
      />
      <OptionCardsDemo />
    </AppShell>
  ),
};

export const BrandMark: StoryObj = {
  render: () => (
    <div style={{ padding: 32, background: "var(--df-navy-800)" }}>
      <BrandLogo />
    </div>
  ),
};

function OptionCardsDemo() {
  const [sel, setSel] = useState("file");
  return (
    <div className="df-option-grid" style={{ maxWidth: 560 }}>
      <SelectOptionCard
        selected={sel === "file"}
        title="Upload file"
        hint="CSV, Excel, JSON, Parquet, PDF, Word"
        onSelect={() => setSel("file")}
      />
      <SelectOptionCard
        selected={sel === "database"}
        title="Connect database"
        hint="PostgreSQL, Snowflake, MongoDB, …"
        onSelect={() => setSel("database")}
      />
      <div className="df-page-actions">
        <Button variant="secondary">Back</Button>
        <Button variant="primary">Continue</Button>
      </div>
    </div>
  );
}
