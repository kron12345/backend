import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { PlanningModule } from './planning/planning.module';
import { DatabaseModule } from './database/database.module';

@Module({
  imports: [DatabaseModule, PlanningModule],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule {}
