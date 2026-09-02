import type { NextRequest } from "next/server";
import { bootstrapResponse, logoutResponse, sessionResponse } from "@/lib/server";

export const dynamic = "force-dynamic";

export function GET(request: NextRequest) { return sessionResponse(request); }
export function POST(request: NextRequest) { return bootstrapResponse(request); }
export function DELETE() { return logoutResponse(); }
