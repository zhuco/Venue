import { NextRequest, NextResponse } from "next/server";
import { jsonHeaders, resolveKolInvite } from "@/lib/server";

export const dynamic = "force-dynamic";

export async function GET(_request: NextRequest, context: { params: Promise<{ inviteCode: string }> }) {
  const { inviteCode } = await context.params;
  const upstream = await resolveKolInvite(inviteCode);
  const body = await upstream.json().catch(() => undefined);
  if (!upstream.ok || !body) return NextResponse.json({ error: "invite_not_found" }, { status: upstream.status === 503 ? 503 : 404, headers: jsonHeaders() });
  return NextResponse.json(body, { headers: jsonHeaders() });
}
