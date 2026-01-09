document.addEventListener("DOMContentLoaded", () => {
  const el = document.getElementById('data-atual');
  // Se o servidor não forneceu a data, preenche com a data de hoje
  if (!el.textContent.trim()) {
    const hoje = new Date();
    const opcoes = { day: '2-digit', month: '2-digit', year: 'numeric' };
    const dataFormatada = hoje.toLocaleDateString('pt-BR', opcoes);
    el.textContent = ` - ${dataFormatada}`;
  }
});
