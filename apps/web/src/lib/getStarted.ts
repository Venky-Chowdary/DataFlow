/**
 * Where the public "Get started" button goes (UX-20). It opened Sign in, which
 * is the wrong door for someone who has no account yet. A signed-in visitor
 * goes to the app; a stranger goes to Sign up when this deployment offers it,
 * and to Sign in (with its own "no account?" guidance) when it does not.
 */
export type GetStartedStage = "app" | "signup" | "login";

export function getStartedStage(signedIn: boolean, signupEnabled: boolean): GetStartedStage {
  if (signedIn) return "app";
  return signupEnabled ? "signup" : "login";
}
