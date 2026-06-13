import { createContext, useContext, useState, ReactNode } from "react";

import { ActiveDataContext } from "./types";

export type { ActiveDataContext };

interface DataContextValue {
  activeData: ActiveDataContext | null;
  setActiveData: (data: ActiveDataContext | null) => void;
}

const DataContext = createContext<DataContextValue>({
  activeData: null,
  setActiveData: () => {},
});

export function DataProvider({ children }: { children: ReactNode }) {
  const [activeData, setActiveData] = useState<ActiveDataContext | null>(null);
  return (
    <DataContext.Provider value={{ activeData, setActiveData }}>
      {children}
    </DataContext.Provider>
  );
}

export function useActiveData() {
  return useContext(DataContext);
}
