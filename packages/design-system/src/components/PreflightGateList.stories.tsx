import type { Meta, StoryObj } from "@storybook/react";
import { PreflightGateList } from "./PreflightGateList";

const meta: Meta<typeof PreflightGateList> = {
  title: "DataFlow/PreflightGateList",
  component: PreflightGateList,
};

export default meta;
type Story = StoryObj<typeof PreflightGateList>;

export const AllPassed: Story = {
  args: {
    gates: [
      { id: "g1", label: "G1 Source readable", status: "pass", message: "47 columns detected", durationMs: 4.2 },
      { id: "g2", label: "G2 Destination reachable", status: "pass", message: "Snowflake write access OK", durationMs: 12.1 },
      { id: "g4", label: "G4 Mapping confidence", status: "pass", message: "44/44 mappings above 0.85", durationMs: 2.0 },
    ],
  },
};

export const Blocked: Story = {
  args: {
    gates: [
      { id: "g1", label: "G1 Source readable", status: "pass", message: "File parsed", durationMs: 3.1 },
      { id: "g4", label: "G4 Mapping confidence", status: "block", message: "Fld_07→payment_amount (0.62)", durationMs: 1.8 },
    ],
  },
};
