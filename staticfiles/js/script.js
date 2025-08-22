document.addEventListener("DOMContentLoaded", () => {
  const hoje = new Date();
  const opcoes = { day: '2-digit', month: '2-digit', year: 'numeric' };
  const dataFormatada = hoje.toLocaleDateString('pt-BR', opcoes);

  document.getElementById('data-atual').textContent = ` - ${dataFormatada}`;
});
