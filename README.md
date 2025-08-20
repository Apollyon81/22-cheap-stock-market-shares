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
