import { MarketingChrome } from "../../components/marketing/MarketingChrome";
import { MarketingSubpage } from "./MarketingSubpage";
import { LandingHome } from "../LandingPage";
import type { PublicRoute } from "../../lib/publicNavigation";

interface MarketingSiteProps {
  route: PublicRoute;
  onNavigate: (route: PublicRoute) => void;
  onLogin: () => void;
  onGetStarted: () => void;
}

export function MarketingSite({ route, onNavigate, onLogin, onGetStarted }: MarketingSiteProps) {
  return (
    <MarketingChrome route={route} onNavigate={onNavigate} onLogin={onLogin} onGetStarted={onGetStarted}>
      {route === "home" ? (
        <LandingHome onLogin={onLogin} onGetStarted={onGetStarted} onNavigate={onNavigate} />
      ) : (
        <MarketingSubpage route={route} onLogin={onLogin} onGetStarted={onGetStarted} onNavigate={onNavigate} />
      )}
    </MarketingChrome>
  );
}
