export const messages: Record<string, string> = { invalid_input: "请检查填写内容。", invalid_login: "用户名或密码不正确。", username_unavailable: "用户名已被使用。", unauthorized: "请重新登录。", forbidden: "当前账户没有此操作权限。", not_found: "未找到记录或邀请已失效。", conflict: "状态已变化，请刷新后重试。", verification_required: "账户验证或启动条件尚未满足。", account_in_use: "账户仍有任务或订单，暂时不能修改。", rate_limited: "请求过于频繁，请稍后再试。", unavailable: "服务暂时不可用。请先刷新状态，勿重复提交。" };
export class RequestError extends Error { constructor(readonly status: number, code: string) { super(messages[code] ?? messages.unavailable); } }
export async function api<T>(action: string, csrf?: string, body?: unknown): Promise<T> {
  let result: Response;
  try {
    result = await fetch(`/api/customer/${action}`, { method: body === undefined ? "GET" : "POST", cache: "no-store", credentials: "same-origin", headers: body === undefined ? {} : { "Content-Type": "application/json", "x-venue-csrf": csrf ?? "" }, body: body === undefined ? undefined : JSON.stringify(body), signal: AbortSignal.timeout(35_000) });
  } catch { throw new RequestError(503, "unavailable"); }
  const value = await result.json().catch(() => { throw new RequestError(503, "unavailable"); });
  if (!result.ok) throw new RequestError(result.status, value.code);
  return value as T;
}
