import { test, expect } from "@playwright/test";
import { mkdirSync } from "node:fs";
import { join } from "node:path";

test("managed account dialog saves, retries with the same identity, verifies and clears secrets", async ({page}, info) => {
  const accounts: object[] = []; const saves: Record<string,string>[] = []; let lost = true; let probes = 0;
  await page.route("**/api/customer/**", async route => {
    const action = new URL(route.request().url()).pathname.split("/").at(-1);
    let value: unknown = null;
    if(action === "session") value={user:{user_id:"kol-fixture",username:"kol"},credentials:[],selected_credential_id:null,csrf:"owned-csrf"};
    else if(action === "kol-profile") value={state:"enabled"};
    else if(action === "kol-invite") value=null;
    else if(action === "kol-source") value={trading_account_id:null,revision:1,can_change:true};
    else if(action === "leader") value={can_use:false,bot:null};
    else if(action === "settings") value=null;
    else if(action === "mirror-orders") value=[];
    else if(action === "managed-followers") {
      if(route.request().method() === "GET") value={can_manage:true,accounts};
      else {
        const body=route.request().postDataJSON(); saves.push(body);
        if(lost) { lost=false; await route.fulfill({status:503,json:{code:"unavailable"}}); return; }
        value={managed_id:body.request_id,label:body.label,masked_key:"••••1234",verification:"unverified",verified_ms:null,equity:null,available_margin:null,balance_observed_ms:null}; accounts.push(value as object);
      }
    } else if(action === "managed-verify") { probes++; value={...accounts[0],verification:"verified",verified_ms:Date.now(),equity:"10",available_margin:"8",balance_observed_ms:Date.now()}; accounts[0]=value as object; }
    else if(action === "managed-delete") { accounts.splice(0,1); value={can_manage:true,accounts}; }
    else throw new Error(`Unexpected route ${action}`);
    await route.fulfill({json:value});
  });
  await page.goto("/login");
  await page.getByRole("button",{name:"添加托管 API密钥",exact:true}).click();
  const dialog=page.getByRole("dialog"); await expect(dialog).toBeVisible();
  const screenshots=process.env.VENUE_WEB_SCREENSHOT_DIR!; mkdirSync(screenshots,{recursive:true});
  await page.screenshot({path:join(screenshots,`managed-dialog-${info.project.name}.png`)});
  expect(await dialog.evaluate(element => element.scrollWidth <= element.clientWidth)).toBeTruthy();
  await dialog.getByLabel("账户 1 标签",{exact:true}).fill("托管一号");
  await dialog.getByLabel("账户 1 API密钥",{exact:true}).fill("K".repeat(32));
  await dialog.getByLabel("账户 1 密钥",{exact:true}).fill("S".repeat(32));
  await expect(dialog.getByLabel("账户 1 跟单方式",{exact:true})).toHaveValue("proportional");
  await expect(dialog.getByLabel("账户 1 跟单倍数",{exact:true})).toHaveValue("1");
  await dialog.getByRole("button",{name:"加密保存账户",exact:true}).click();
  await expect(dialog.getByText("结果待确认，请重试原内容；不会重复创建。",{exact:true})).toBeVisible();
  await expect(dialog.getByLabel("账户 1 API密钥",{exact:true})).toBeDisabled();
  await dialog.getByRole("button",{name:"确认并重试保存",exact:true}).click();
  await expect(dialog).not.toBeVisible();
  expect(saves).toHaveLength(2); expect(saves[0]).toEqual(saves[1]);
  expect(saves[0].authorization).toEqual({sizing:{mode:"proportional"},multiplier:"1"});
  const panel=page.getByRole("region",{name:"托管跟单账户"});
  await expect(panel.getByRole("cell",{name:"托管一号",exact:true})).toBeVisible();
  const verify = panel.getByRole("button",{name:"验证权限并申请跟单",exact:true}); await verify.focus(); await verify.press("Enter");
  await expect(panel.getByRole("cell",{name:"验证通过",exact:true})).toBeVisible(); expect(probes).toBe(1);
  await expect(panel.getByRole("cell",{name:"10 USD",exact:true})).toBeVisible();
  await expect(panel.getByRole("cell",{name:"8 USD",exact:true})).toBeVisible();
  await expect(panel.getByRole("button",{name:"跟单设置",exact:true})).toBeVisible();
  const remove = panel.getByRole("button",{name:"删除托管账户",exact:true}); await remove.focus(); await remove.press("Enter");
  await expect(panel.getByText("尚未添加托管账户。",{exact:false})).toBeVisible();
  expect(await page.evaluate(()=>JSON.stringify({...localStorage,...sessionStorage}))).not.toContain("S".repeat(32));
});
