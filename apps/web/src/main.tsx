import React from "react";
import ReactDOM from "react-dom/client";
import "@fontsource/plus-jakarta-sans/400.css";
import "@fontsource/plus-jakarta-sans/500.css";
import "@fontsource/plus-jakarta-sans/600.css";
import "@fontsource/plus-jakarta-sans/700.css";
import "@fontsource/jetbrains-mono/400.css";
import "@fontsource/jetbrains-mono/500.css";
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
