/**
 * Defaults for Settings → Team → Add member.
 *
 * The email invite flow (membership only, no password) is the default. Minting
 * a login puts a one-time password on screen, so it is an explicit choice.
 */
export const INVITE_DEFAULTS = {
  createLogin: false,
} as const;
