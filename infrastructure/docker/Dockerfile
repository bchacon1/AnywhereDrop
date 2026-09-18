# Multi-stage build: frontend -> embedded into the Go binary -> minimal runtime.
# Build context is the repository root:  docker build -f infrastructure/docker/Dockerfile .

FROM node:22-alpine AS frontend
WORKDIR /src/frontend
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build

FROM golang:1.27-alpine AS backend
WORKDIR /src/backend
COPY backend/go.mod backend/go.sum* ./
RUN go mod download
COPY backend/ ./
COPY --from=frontend /src/frontend/dist ./web/dist
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/server ./cmd/server

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=backend /out/server /server
ENV PORT=8080
EXPOSE 8080
USER nonroot
ENTRYPOINT ["/server"]
