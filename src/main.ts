import 'dotenv/config';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { FastifyAdapter } from '@nestjs/platform-fastify';
import { ValidationPipe } from '@nestjs/common';
import { DocumentBuilder, SwaggerModule } from '@nestjs/swagger';
import fastifySse from 'fastify-sse-v2';

async function bootstrap() {
  const fastifyAdapter = new FastifyAdapter();
  await fastifyAdapter.register(fastifySse);
  const app = await NestFactory.create(AppModule, fastifyAdapter);
  app.setGlobalPrefix('api/v1');
  const allowedOrigins = [/^https?:\/\/localhost(?::\d+)?$/, /\.animeland\.de$/];
  app.enableCors({
    origin: (origin, callback) => {
      if (!origin) {
        callback(null, true);
        return;
      }
      const isAllowed = allowedOrigins.some((pattern) => pattern.test(origin));
      callback(isAllowed ? null : new Error('Origin not allowed by CORS'), isAllowed);
    },
    credentials: true,
    methods: ['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS', 'HEAD'],
    allowedHeaders: [
      'Content-Type',
      'Authorization',
      'Accept',
      'Origin',
      'If-Match',
      'If-None-Match',
      'X-Requested-With',
      'X-Client-Request-Id',
    ],
    exposedHeaders: ['ETag', 'Location'],
    maxAge: 3600,
  });
  app.useGlobalPipes(new ValidationPipe({ whitelist: true, transform: true }));

  const config = new DocumentBuilder()
    .setTitle('Planning API')
    .setVersion('1.0.0')
    .build();
  const doc = SwaggerModule.createDocument(app, config);
  SwaggerModule.setup('/api/docs', app, doc);

  await app.listen(3000, '0.0.0.0');
}
bootstrap();
