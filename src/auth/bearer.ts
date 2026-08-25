import type { Env } from '../types';
import { timingSafeEqual } from '../utils/crypto';

export function verifyBearer(request: Request, env: Env): boolean | Promise<boolean> {
  const auth = request.headers.get('Authorization');
  if (!auth) return false;
  const parts = auth.split(' ');
  if (parts.length !== 2 || parts[0] !== 'Bearer') return false;

  // Validate as Telegram WebApp initData (HMAC-SHA256)
  return verifyTelegramInitData(parts[1], env);
}

/**
 * Validate Telegram WebApp initData per:
 * https://core.telegram.org/bots/webapps#validating-data-received-via-the-mini-app
 *
 * 1. Parse initData as query params, extract `hash`
 * 2. Sort remaining params alphabetically, join with \n as data_check_string
 * 3. secret_key = HMAC-SHA256("WebAppData", bot_token)
 * 4. Verify HMAC-SHA256(data_check_string, secret_key) === hash
 * 5. Check auth_date freshness (allow up to 1 hour)
 * 6. If TG_ADMIN_IDS is set, verify user ID is allowed
 */
async function verifyTelegramInitData(initData: string, env: Env): Promise<boolean> {
  try {
    const params = new URLSearchParams(initData);
    const hash = params.get('hash');
    if (!hash) return false;

    // auth_date is mandatory per Telegram docs; reject if missing
    const authDate = params.get('auth_date');
    if (!authDate) return false;
    const age = Math.floor(Date.now() / 1000) - parseInt(authDate, 10);
    if (isNaN(age) || age > 3600 || age < -60) return false;

    // Build data_check_string: sorted params excluding hash, joined by \n
    params.delete('hash');
    const entries = Array.from(params.entries()).sort(([a], [b]) => a.localeCompare(b));
    const dataCheckString = entries.map(([k, v]) => `${k}=${v}`).join('\n');

    // secret_key = HMAC-SHA256("WebAppData", bot_token)
    const encoder = new TextEncoder();
    const secretKey = await crypto.subtle.importKey(
      'raw', encoder.encode('WebAppData'), { name: 'HMAC', hash: 'SHA-256' }, false, ['sign'],
    );
    const secretBuf = await crypto.subtle.sign('HMAC', secretKey, encoder.encode(env.TG_BOT_TOKEN));

    // computed_hash = HMAC-SHA256(data_check_string, secret_key)
    const signingKey = await crypto.subtle.importKey(
      'raw', secretBuf, { name: 'HMAC', hash: 'SHA-256' }, false, ['sign'],
    );
    const sig = await crypto.subtle.sign('HMAC', signingKey, encoder.encode(dataCheckString));

    // Compare hex (timing-safe)
    const computed = Array.from(new Uint8Array(sig), b => b.toString(16).padStart(2, '0')).join('');
    if (computed.length !== hash.length || !timingSafeEqual(computed, hash)) {
      return false;
    }

    // If TG_ADMIN_IDS is configured, restrict access to allowed user IDs
    if (env.TG_ADMIN_IDS) {
      const userStr = params.get('user');
      if (!userStr) return false;
      let userId: number | undefined;
      try {
        const userObj = JSON.parse(userStr);
        userId = userObj?.id;
      } catch {
        return false;
      }
      if (!userId) return false;
      const allowed = env.TG_ADMIN_IDS.split(',').map(id => id.trim());
      if (!allowed.includes(userId.toString())) {
        return false;
      }
    }

    return true;
  } catch {
    return false;
  }
}
