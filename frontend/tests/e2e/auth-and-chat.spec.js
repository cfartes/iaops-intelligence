import { expect, test } from "@playwright/test";
import { mockDefaultApi } from "./helpers/mockApi";

async function dismissModalIfPresent(page) {
  const overlay = page.locator(".modal-overlay");
  if ((await overlay.count()) > 0) {
    const postpone = overlay.getByRole("button", { name: /Lembrar mais tarde|Remind me later/i });
    if (await postpone.count()) {
      await postpone.first().click({ force: true });
      await overlay.first().waitFor({ state: "hidden", timeout: 5000 });
      return;
    }
    const close = overlay.getByRole("button", { name: /Fechar|Close/i });
    if (await close.count()) {
      await close.first().click({ force: true });
      await overlay.first().waitFor({ state: "hidden", timeout: 5000 });
      return;
    }
    const fallback = overlay.locator("button").first();
    if (await fallback.isVisible()) {
      await fallback.click({ force: true });
    }
  }
}

test("login and run chat query", async ({ page }) => {
  await mockDefaultApi(page);
  await page.addInitScript(() => {
    window.localStorage.setItem("iaops_setup_assistant_defer_until_v1", String(Date.now() + 7 * 24 * 60 * 60 * 1000));
  });
  await page.goto("/");

  await page.getByRole("button", { name: /entrar|sign in/i }).first().click();
  await page.getByLabel(/e-mail de acesso|access e-mail/i).fill("owner@test.local");
  await page.getByLabel(/senha|password/i).fill("Strong@1234");
  await page.locator("form").getByRole("button", { name: /^entrar$|^sign in$/i }).click();
  await dismissModalIfPresent(page);

  await expect(page.getByText(/Chat BI/i)).toBeVisible();
  await page.getByText(/^Chat BI$/).first().click();
  await page.getByLabel(/Pergunta|Question/i).fill("Quantos incidentes abertos temos agora?");
  await page.getByRole("button", { name: /Perguntar|Ask/i }).click();
  await expect(page.getByText(/Total encontrado|Consulta concluida|record/i)).toBeVisible();
});

test("chat shows friendly tenant blocked message", async ({ page }) => {
  await mockDefaultApi(page);
  await page.addInitScript(() => {
    window.localStorage.setItem("iaops_setup_assistant_defer_until_v1", String(Date.now() + 7 * 24 * 60 * 60 * 1000));
  });
  await page.goto("/");

  await page.getByRole("button", { name: /entrar|sign in/i }).first().click();
  await page.getByLabel(/e-mail de acesso|access e-mail/i).fill("owner@test.local");
  await page.getByLabel(/senha|password/i).fill("Strong@1234");
  await page.locator("form").getByRole("button", { name: /^entrar$|^sign in$/i }).click();
  await dismissModalIfPresent(page);

  await page.getByText(/^Chat BI$/).first().click();
  await page.getByLabel(/Pergunta|Question/i).fill("mostrar cpf dos clientes");
  await page.getByRole("button", { name: /Perguntar|Ask/i }).click();

  await expect(page.getByRole("heading", { name: /Tenant bloqueado/i })).toBeVisible();
});
