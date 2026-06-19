FROM node:20-alpine AS builder
WORKDIR /app
COPY webapp/package.json ./
RUN npm install --frozen-lockfile
COPY webapp/ .
ARG VITE_API_BASE=http://localhost:8001
ARG VITE_ROUTING_DB_BASE=http://localhost:8000
ENV VITE_API_BASE=$VITE_API_BASE
ENV VITE_ROUTING_DB_BASE=$VITE_ROUTING_DB_BASE
RUN npm run build

FROM nginx:1.27-alpine
COPY --from=builder /app/dist /usr/share/nginx/html
COPY webapp/nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
