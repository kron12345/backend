import { Module } from '@nestjs/common';
import { PlanningController } from './planning.controller';
import { PlanningMasterDataController } from './planning-master-data.controller';
import { PlanningService } from './planning.service';
import { PlanningRepository } from './planning.repository';

@Module({
  controllers: [PlanningController, PlanningMasterDataController],
  providers: [PlanningService, PlanningRepository],
  exports: [PlanningService],
})
export class PlanningModule {}
