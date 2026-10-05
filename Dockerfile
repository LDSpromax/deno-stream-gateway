FROM denoland/deno:latest

WORKDIR /app

COPY deno.js .

ENV PORT=8080
EXPOSE 8080

CMD ["deno", "run", "--allow-all", "deno.js"]
