import { test, expect } from "@playwright/test";
import { mkdirSync } from "node:fs";
import { join } from "node:path";

test("managed account dialog saves, retries with the same identity, verifies and clears secrets", async ({page}, info) => {
  const accounts: object[] = []; const saves: Record<string,string>[] = []; let lost = true; let probes = 0;
  await page.route("**/api/customer/**", async route => {
    const action = new URL(route.request().url()).pathname.split("/").at(-1);
    let value: unknown = null;
    if(action === "session") value={user:{user_id:"kol-fixture",username:"kol"},credentials:[],selected_credential_id:null,csrf:"owned-csrf"};
    else if(action === "leader") value={can_use:false,bot:null};
    else if(action === "settings") value=null;
    else if(action === "mirror-orders") value=[];
    else if(action === "managed-followers") {
      if(route.request().method() === "GET") value={can_manage:true,accounts};
      else {
        const body=route.request().postDataJSON(); saves.push(body);
        if(lost) { lost=false; await route.fulfill({status:503,json:{code:"unavailable"}}); return; }
        value={managed_id:body.request_id,label:body.label,masked_key:"••••1234",verification:"unverified",verified_ms:null}; accounts.push(value as object);
      }
    } else if(action === "managed-verify") { probes++; value={...accounts[0],verification:"verified",verified_ms:Date.now()}; accounts[0]=value as object; }
    else throw new Error(`Unexpected route ${action}`);
    await route.fulfill({json:value});
  });
  await page.goto("/login");
  await page.getByRole("button",{name:"添加托管 API Key",exact:true}).click();
  const dialog=page.getByRole("dialog"); await expect(dialog).toBeVisible();
  const screenshots=process.env.VENUE_WEB_SCREENSHOT_DIR!; mkdirSync(screenshots,{recursive:true});
  await page.screenshot({path:join(screenshots,`managed-dialog-${info.project.name}.png`)});
  expect(await dialog.evaluate(element => element.scrollWidth <= element.clientWidth)).toBeTruthy();
  await dialog.getByLabel("账户 1 标签",{exact:true}).fill("托管一号");
  await dialog.getByLabel("账户 1 API Key",{exact:true}).fill("K".repeat(32));
  await dialog.getByLabel("账户 1 API Secret",{exact:true}).fill("S".repeat(32));
  await dialog.getByRole("button",{name:"加密保存账户",exact:true}).click();
  await expect(dialog.getByText("结果待确认，请重试原内容；不会重复创建。",{exact:true})).toBeVisible();
  await expect(dialog.getByLabel("账户 1 API Key",{exact:true})).toBeDisabled();
  await dialog.getByRole("button",{name:"确认并重试保存",exact:true}).click();
  await expect(dialog.getByText("已加密保存",{exact:true})).toBeVisible();
  expect(saves).toHaveLength(2); expect(saves[0]).toEqual(saves[1]);
  await expect(dialog.getByLabel("账户 1 API Secret",{exact:true})).toHaveValue("");
  await dialog.getByRole("button",{name:"完成",exact:true}).click();
  const panel=page.getByRole("region",{name:"托管跟单账户"});
  await expect(panel.getByRole("cell",{name:"托管一号",exact:true})).toBeVisible();
  await panel.getByRole("button",{name:"验证权限",exact:true}).click();
  await expect(panel.getByRole("cell",{name:"验证通过",exact:true})).toBeVisible(); expect(probes).toBe(1);
  await expect(panel.getByRole("button",{name:"跟单设置",exact:true})).toBeVisible();
  expect(await page.evaluate(()=>JSON.stringify({...localStorage,...sessionStorage}))).not.toContain("S".repeat(32));
});
