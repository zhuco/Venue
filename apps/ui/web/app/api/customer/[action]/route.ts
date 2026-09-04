import { NextRequest } from "next/server";
import { customerResponse } from "@/lib/customer-server";

export const runtime = "nodejs";
type Context = { params: Promise<{ action: string }> };
export async function GET(request: NextRequest, context: Context) { return customerResponse(request, (await context.params).action); }
export async function POST(request: NextRequest, context: Context) { return customerResponse(request, (await context.params).action); }
