import { notFound } from "next/navigation";
import { KolJoin } from "@/components/kol-join";
import { resolveKolInvite } from "@/lib/server";

export const dynamic = "force-dynamic";

type Invite = { profile?: { name?: string; title?: string; description?: string; state?: string; }; };

export default async function JoinPage({ params }: { params: Promise<{ inviteCode: string }> }) {
  const { inviteCode } = await params;
  const response = await resolveKolInvite(inviteCode);
  const invite = await response.json().catch(() => undefined) as Invite | undefined;
  const profile = invite?.profile;
  if (!response.ok || !profile || profile.state !== "enabled" || typeof profile.name !== "string" || typeof profile.title !== "string" || typeof profile.description !== "string") notFound();
  return <KolJoin inviteCode={inviteCode} profile={{ name: profile.name, title: profile.title, description: profile.description }} />;
}
