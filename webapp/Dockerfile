FROM node:12.18.1 as build

ENV NODE_ENV=production

COPY . /app

WORKDIR /app

RUN npm install 

ENV PATH /app/node_modules/.bin:$PATH

RUN npm run build

FROM nginx:stable-alpine

COPY --from=build /app/build /usr/share/nginx/html

EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]