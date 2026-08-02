import React from "react";
import ReactDOM from "react-dom/client";
import "@fontsource/plus-jakarta-sans/400.css";
import "@fontsource/plus-jakarta-sans/500.css";
import "@fontsource/plus-jakarta-sans/600.css";
import "@fontsource/plus-jakarta-sans/700.css";
import "@fontsource/ibm-plex-sans/400.css";
import "@fontsource/ibm-plex-sans/500.css";
import "@fontsource/ibm-plex-sans/600.css";
import "@fontsource/jetbrains-mono/400.css";
import "@fontsource/jetbrains-mono/500.css";
import { DataTransferApp } from "./DataTransferApp";
import { PageErrorBoundary } from "./components/PageErrorBoundary";
import "./styles/app-styles.css";

/* ToastProvider lives once inside DataTransferApp — mounting it here too
   created a second .dt-toast-host and duplicate notifications. */
ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <PageErrorBoundary label="DataFlow">
      <DataTransferApp />
    </PageErrorBoundary>
  </React.StrictMode>
);
