import { CustomerConsole } from "@/components/customer-console";
export default async function Page({ params }: { params: Promise<{ invite_code: string }> }) {
  return <CustomerConsole inviteCode={(await params).invite_code} />;
}
