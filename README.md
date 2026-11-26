# Vítreo Relay

Servidor de conexão do **Acesso Remoto** do Vítreo.

Os dois computadores se conectam a este servidor por WebSocket (saída HTTPS), o que funciona atrás de qualquer roteador ou CGNAT.
Todo o conteúdo da sessão é criptografado de ponta a ponta (AES-256-GCM, chave derivada da senha): o servidor apenas repassa bytes que não consegue ler.

- `GET /` → status (`Vitreo Relay OK - N online`)
- `GET /ws?role=host&id=123456789` → computador que compartilha a tela
- `GET /ws?role=viewer&id=123456789` → computador que acessa

Deploy: qualquer serviço que rode Docker (Render, Koyeb, Fly.io…). A porta vem da variável `PORT`.
