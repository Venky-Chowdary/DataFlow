import type { Preview } from "@storybook/react";
import "../src/styles.css";

const preview: Preview = {
  parameters: {
    backgrounds: {
      default: "light",
      values: [
        { name: "light", value: "#F8F9FB" },
        { name: "dark", value: "#0F1419" },
      ],
    },
  },
};

export default preview;
