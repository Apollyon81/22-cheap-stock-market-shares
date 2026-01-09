# 📊 Projeto: Seleção das 20 Ações Mais Baratas da Bolsa

*Objetivo:*  
Desenvolver um site que apresenta uma lista atualizada das 20 ações mais baratas da B3, com base em critérios fundamentalistas sólidos e transparentes.

---

## ✅ Critérios de Seleção

### 1. Base de Dados
- Considerar *todas as ações da B3* inicialmente.

### 2. Filtros Aplicados
- *Liquidez mínima:*
  - Volume médio diário ≥ R$ 1 milhão (para publicação pública)  
  - Volume ≥ R$ 5 milhões (em backtests)  
  - Fonte: Scribd, Reddit

- *Sem recuperação judicial ou irregularidades contábeis:*
  - Excluir empresas com indícios de fraude contábil ou em recuperação judicial  
  - Fonte: Relatos diversos

- *Lucros consistentes:*
  - EBIT (lucro operacional) positivo  
  - LPA (lucro por ação) positivo  
  - Fonte: Reddit

- *Exclusão de lucros não recorrentes:*
  - Eliminar empresas com ganhos extraordinários que distorçam o Earning Yield

### 3. Métrica Principal
- *Earning Yield (EBIT / EV)*
  - Mede o lucro operacional em relação ao valor da firma
  - Também conhecido como inverso do EV/EBIT
  - Superior ao tradicional P/VPA
  - Fonte: InfoMoney, Valor Investe, Clube do Valor

### 4. Outros Ajustes
- Ordenação final em *ordem alfabética*
- Reservar *espaço para publicidade* na página

---

## 🔁 Meta Diária

- Realizar *pelo menos 1 commit por dia* no repositório do projeto

---

## 📅 Prazo

- Conclusão estimada: *30 dias*

---

## 🔍 Diagnóstico de bloqueios (HTTP 403)

Se o scraping estiver retornando HTTP 403 (Forbidden) em produção, é útil habilitar logs verbosos temporariamente para diagnosticar a causa.

- Variável: `SCRAPE_VERBOSE_LOGGING=1` (habilita logs adicionais em `views` e no comando `scrape_data`).
- O que é logado (resumido): trechos dos `response` headers (Server, X-Cache, Content-Type), um snippet seguro do corpo da resposta (até 1000 caracteres) e os `request` headers relevantes (User-Agent, Accept, Referer) — nada de credenciais.
- Use junto com `SCRAPE_HTTP_MAX_ATTEMPTS=1` para testar com menos tentativas e ver rapidamente os logs.

Possíveis causas que os logs ajudam a identificar:
- Bloqueio por IP (Render): procure por headers como `Via`, `X-Cache` ou por mensagens no body do servidor.
- Bloqueio por User-Agent: compare o `User-Agent` enviado com o que aparece nos logs.
- Bloqueio por frequência: observe se o site retorna 403 intermitente; ajuste `SCRAPE_BACKOFF_BASE_HOURS` / `SCRAPE_BACKOFF_MAX_HOURS` em produção.
- Proteção anti-bot: mensagens no corpo podem indicar detecção de bot/Selenium.

Desabilite `SCRAPE_VERBOSE_LOGGING` após coleta de logs — ele é só para diagnóstico temporário.
