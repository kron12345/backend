import { Module } from '@nestjs/common';
import { PlanningController } from './planning.controller';
import { PlanningMasterDataController } from './planning-master-data.controller';
import { PlanningService } from './planning.service';

@Module({
  controllers: [PlanningController, PlanningMasterDataController],
  providers: [PlanningService],
  exports: [PlanningService],
})
export class PlanningModule {}
