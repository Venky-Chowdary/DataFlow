import { useState } from "react";
import { AppLayout, type PipelineStepItem } from "@dataflow/design-system";
import { emptyEndpoint } from "./lib/types";
import { ConnectorsScreen, OperationsScreen } from "./screens/PlatformScreens";
import { TransferSelectScreen, type TransferDraft } from "./screens/TransferSelectScreen";
import { TransferScreen } from "./screens/TransferScreen";
import type { NavItemId, WizardStep } from "./lib/types";

const PIPELINE_STEPS: PipelineStepItem[] = [
  { id: "select", label: "Source & destination" },
  { id: "execute", label: "Preflight & transfer" },
];

function initialDraft(): TransferDraft {
  return {
    templateId: "file-db",
    source: emptyEndpoint("file", "Source"),
    destination: emptyEndpoint("database", "Destination"),
    exportFormat: "csv",
    apiUrl: "",
    sourceDbType: "",
    destDbType: "",
    destConnectorId: "",
    sourceConnectorId: "",
  };
}

export default function App() {
  const [nav, setNav] = useState<NavItemId>("transfer");
  const [wizardStep, setWizardStep] = useState<WizardStep>("connect");
  const [draft, setDraft] = useState<TransferDraft>(initialDraft);

  const inPipeline = nav === "transfer";
  const stepIndex = wizardStep === "transfer" ? 1 : 0;

  function startTransfer() {
    setWizardStep("connect");
    setDraft(initialDraft());
    setNav("transfer");
  }

  function handleNavigate(id: NavItemId) {
    if (id === "transfer") startTransfer();
    else setNav(id);
  }

  return (
    <AppLayout
      activeNav={nav}
      onNavigate={handleNavigate}
      onNewTransfer={startTransfer}
      narrow={inPipeline && wizardStep === "transfer"}
      pipeline={
        inPipeline
          ? {
              steps: PIPELINE_STEPS,
              currentIndex: stepIndex,
              onExit: () => setNav("jobs"),
            }
          : undefined
      }
    >
      {inPipeline ? (
        <div key={wizardStep}>
          {wizardStep === "connect" && (
            <TransferSelectScreen
              draft={draft}
              onDraftChange={setDraft}
              onContinue={() => setWizardStep("transfer")}
            />
          )}
          {wizardStep === "transfer" && (
            <TransferScreen draft={draft} onBack={() => setWizardStep("connect")} />
          )}
        </div>
      ) : (
        <>
          {nav === "jobs" && <OperationsScreen onNewTransfer={startTransfer} />}
          {nav === "connectors" && <ConnectorsScreen onNewTransfer={startTransfer} />}
        </>
      )}
    </AppLayout>
  );
}
