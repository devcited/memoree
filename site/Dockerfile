FROM node:24-alpine AS build

ENV ASTRO_TELEMETRY_DISABLED=1
WORKDIR /app
RUN corepack enable && corepack prepare pnpm@10.15.1 --activate

COPY site/package.json site/pnpm-lock.yaml ./site/
RUN cd site && pnpm install --frozen-lockfile

COPY docs ./docs
COPY site ./site
RUN cd site && pnpm build

FROM nginxinc/nginx-unprivileged:1.29.1-alpine

COPY site/nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/site/dist /usr/share/nginx/html

EXPOSE 8080
