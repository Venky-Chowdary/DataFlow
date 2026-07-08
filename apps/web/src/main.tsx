import React from "react";
import ReactDOM from "react-dom/client";
import { DataTransferApp } from "./DataTransferApp";
import "./styles/tokens.css";
import "./styles/jarvis-ui.css";
import "./styles/landing.css";
import "./styles/datatransfer-design.css";
import "./styles/dataflow-ui.css";

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <DataTransferApp />
  </React.StrictMode>
);
