import { createContext, useContext, useState, type Dispatch, type ReactNode, type SetStateAction } from "react";

import { ActiveDataContext } from "./types";

export type { ActiveDataContext };

interface DataContextValue {
  activeData: ActiveDataContext | null;
  setActiveData: Dispatch<SetStateAction<ActiveDataContext | null>>;
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
