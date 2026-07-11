import React from "react";
import ReactDOM from "react-dom/client";
import { DataTransferApp } from "./DataTransferApp";
import { PageErrorBoundary } from "./components/PageErrorBoundary";
import "./styles/app-styles.css";

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <PageErrorBoundary label="DataFlow">
      <DataTransferApp />
    </PageErrorBoundary>
  </React.StrictMode>
);
