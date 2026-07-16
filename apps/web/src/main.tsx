import React from "react";
import ReactDOM from "react-dom/client";
import { DataTransferApp } from "./DataTransferApp";
import { PageErrorBoundary } from "./components/PageErrorBoundary";
import { ToastProvider } from "./components/Toast";
import "./styles/app-styles.css";

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <ToastProvider>
      <PageErrorBoundary label="DataFlow">
        <DataTransferApp />
      </PageErrorBoundary>
    </ToastProvider>
  </React.StrictMode>
);
