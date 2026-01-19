"""
REST API for DataFactory 2.0
Provides endpoints for job management, execution, and monitoring
"""
from fastapi import FastAPI, HTTPException, BackgroundTasks, UploadFile, File
from pydantic import BaseModel
from typing import Optional, Dict, List
from job_manager import JobManager
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime
import uvicorn
import os
import shutil
import uuid

app = FastAPI(
    title="DataFactory 2.0 API",
    description="API for managing data integration jobs",
    version="2.0.0"
)

# Initialize job manager
job_manager = JobManager()

# Initialize scheduler
scheduler = BackgroundScheduler()
scheduler.start()


# Pydantic models for request/response
class JobCreate(BaseModel):
    job_name: str
    source_type: str
    source_config: Dict
    sink_type: str
    sink_config: Dict
    query: Optional[str] = None
    schedule: Optional[str] = None


class JobUpdate(BaseModel):
    job_name: Optional[str] = None
    source_type: Optional[str] = None
    source_config: Optional[Dict] = None
    sink_type: Optional[str] = None
    sink_config: Optional[Dict] = None
    query: Optional[str] = None
    schedule: Optional[str] = None
    enabled: Optional[bool] = None


# Root endpoint
@app.get("/")
async def root():
    """Root endpoint with API information"""
    return {
        "name": "DataFactory 2.0 API",
        "version": "2.0.0",
        "endpoints": {
            "jobs": "/jobs",
            "execute": "/jobs/{job_id}/execute",
            "history": "/history",
            "logs": "/logs/{history_id}",
            "health": "/health"
        }
    }


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "scheduler": "running" if scheduler.running else "stopped"
    }


# Job Management Endpoints
@app.post("/jobs", status_code=201)
async def create_job(job: JobCreate):
    """Create a new data integration job"""
    try:
        job_id = job_manager.create_job(
            job_name=job.job_name,
            source_type=job.source_type,
            source_config=job.source_config,
            sink_type=job.sink_type,
            sink_config=job.sink_config,
            query=job.query,
            schedule=job.schedule
        )
        
        # Schedule the job if schedule is provided
        if job.schedule:
            try:
                scheduler.add_job(
                    func=lambda: job_manager.execute_job(job_id),
                    trigger=CronTrigger.from_crontab(job.schedule),
                    id=f"job_{job_id}",
                    name=job.job_name,
                    replace_existing=True
                )
            except Exception as e:
                # Job created but scheduling failed
                return {
                    "job_id": job_id,
                    "message": "Job created but scheduling failed",
                    "error": str(e)
                }
        
        return {
            "job_id": job_id,
            "message": "Job created successfully",
            "scheduled": job.schedule is not None
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/jobs")
async def list_jobs():
    """List all jobs with their last execution information"""
    try:
        jobs = job_manager.list_jobs_with_last_run()
        return {"jobs": jobs, "count": len(jobs)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/jobs/{job_id}")
async def get_job(job_id: int):
    """Get job details by ID"""
    try:
        job = job_manager.get_job_with_last_run(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        return job
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.put("/jobs/{job_id}")
async def update_job(job_id: int, job_update: JobUpdate):
    """Update job configuration"""
    try:
        # Prepare update data
        update_data = {k: v for k, v in job_update.dict().items() if v is not None}
        
        if not update_data:
            raise HTTPException(status_code=400, detail="No update data provided")
        
        success = job_manager.update_job(job_id, **update_data)
        
        if not success:
            raise HTTPException(status_code=404, detail="Job not found")
        
        # Update schedule if changed
        if 'schedule' in update_data:
            schedule = update_data['schedule']
            job_name = job_manager.get_job(job_id)['job_name']
            
            # Remove old schedule
            try:
                scheduler.remove_job(f"job_{job_id}")
            except:
                pass
            
            # Add new schedule if provided
            if schedule:
                try:
                    scheduler.add_job(
                        func=lambda: job_manager.execute_job(job_id),
                        trigger=CronTrigger.from_crontab(schedule),
                        id=f"job_{job_id}",
                        name=job_name,
                        replace_existing=True
                    )
                except Exception as e:
                    return {
                        "message": "Job updated but scheduling failed",
                        "error": str(e)
                    }
        
        return {"message": "Job updated successfully"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/jobs/{job_id}")
async def delete_job(job_id: int):
    """Delete a job"""
    try:
        # Remove from scheduler
        try:
            scheduler.remove_job(f"job_{job_id}")
        except:
            pass
        
        success = job_manager.delete_job(job_id)
        
        if not success:
            raise HTTPException(status_code=404, detail="Job not found")
        
        return {"message": "Job deleted successfully"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# File Upload Endpoint
@app.post("/upload/file")
async def upload_file(file: UploadFile = File(...)):
    """Upload a file to the server"""
    try:
        # Create uploads directory if not exists
        upload_dir = os.path.join(os.getcwd(), "uploads")
        os.makedirs(upload_dir, exist_ok=True)
        
        # Generate unique filename to avoid collisions
        unique_filename = f"{uuid.uuid4()}_{file.filename}"
        file_path = os.path.join(upload_dir, unique_filename)
        
        # Save file
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
            
        return {
            "filename": unique_filename,
            "original_filename": file.filename,
            "file_path": file_path,
            "message": "File uploaded successfully"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Job Execution Endpoints
@app.post("/jobs/{job_id}/execute")
async def execute_job(job_id: int, background_tasks: BackgroundTasks):
    """Execute a job immediately"""
    try:
        job = job_manager.get_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        
        # Execute in background
        result = job_manager.execute_job(job_id)
        
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/jobs/{job_id}/sink/objects")
async def get_sink_objects(job_id: int):
    """Get list of objects in the job's sink (tables/files)"""
    try:
        objects = job_manager.get_sink_objects(job_id)
        return {"objects": objects}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/jobs/{job_id}/sink/preview")
async def get_sink_preview(job_id: int, object_name: str, limit: int = 100):
    """Get preview data from a sink object"""
    try:
        data = job_manager.get_sink_preview(job_id, object_name, limit)
        return {"data": data, "count": len(data)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



# History and Logging Endpoints
@app.get("/history")
async def get_all_history(limit: int = 100):
    """Get execution history for all jobs"""
    try:
        history = job_manager.get_all_history(limit=limit)
        return {"history": history, "count": len(history)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/jobs/{job_id}/history")
async def get_job_history(job_id: int, limit: int = 50):
    """Get execution history for a specific job"""
    try:
        history = job_manager.get_job_history(job_id, limit=limit)
        return {"history": history, "count": len(history)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/logs/{history_id}")
async def get_job_logs(history_id: int):
    """Get logs for a specific job execution"""
    try:
        logs = job_manager.get_job_logs(history_id)
        if not logs:
            raise HTTPException(status_code=404, detail="No logs found for this execution")
        return {"logs": logs, "count": len(logs)}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Scheduler Endpoints
@app.get("/scheduler/jobs")
async def get_scheduled_jobs():
    """Get all scheduled jobs"""
    try:
        jobs = []
        for job in scheduler.get_jobs():
            jobs.append({
                "id": job.id,
                "name": job.name,
                "next_run": job.next_run_time.isoformat() if job.next_run_time else None,
                "trigger": str(job.trigger)
            })
        return {"scheduled_jobs": jobs, "count": len(jobs)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/scheduler/pause")
async def pause_scheduler():
    """Pause the job scheduler"""
    try:
        scheduler.pause()
        return {"message": "Scheduler paused"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/scheduler/resume")
async def resume_scheduler():
    """Resume the job scheduler"""
    try:
        scheduler.resume()
        return {"message": "Scheduler resumed"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Connector Information Endpoints
@app.get("/connectors")
async def list_connectors():
    """List available source and sink connectors"""
    return {
        "source_connectors": [
            {
                "type": "odbc",
                "description": "ODBC data source (SQL Server, etc.)",
                "config_example": {
                    "dsn": "MyDSN",
                    "database": "MyDatabase",
                    "schema": "dbo"
                }
            },
            {
                "type": "postgresql",
                "description": "PostgreSQL database",
                "config_example": {
                    "host": "localhost",
                    "port": 5432,
                    "database": "mydb",
                    "user": "user",
                    "password": "password",
                    "schema": "public"
                }
            },
            {
                "type": "mysql",
                "description": "MySQL database",
                "config_example": {
                    "host": "localhost",
                    "port": 3306,
                    "database": "mydb",
                    "user": "user",
                    "password": "password"
                }
            },
            {
                "type": "csv",
                "description": "CSV file",
                "config_example": {
                    "file_path": "/path/to/file.csv"
                }
            },
            {
                "type": "json",
                "description": "JSON file",
                "config_example": {
                    "file_path": "/path/to/file.json"
                }
            },
            {
                "type": "excel",
                "description": "Excel file",
                "config_example": {
                    "file_path": "/path/to/file.xlsx",
                    "sheet_name": "Sheet1"
                }
            }
        ],
        "sink_connectors": [
            {
                "type": "sqlite",
                "description": "SQLite database",
                "config_example": {
                    "database_path": "/path/to/database.db"
                }
            },
            {
                "type": "postgresql",
                "description": "PostgreSQL database",
                "config_example": {
                    "host": "localhost",
                    "port": 5432,
                    "database": "mydb",
                    "user": "user",
                    "password": "password"
                }
            },
            {
                "type": "mysql",
                "description": "MySQL database",
                "config_example": {
                    "host": "localhost",
                    "port": 3306,
                    "database": "mydb",
                    "user": "user",
                    "password": "password"
                }
            },
            {
                "type": "csv",
                "description": "CSV files",
                "config_example": {
                    "directory": "/path/to/output"
                }
            },
            {
                "type": "json",
                "description": "JSON files",
                "config_example": {
                    "directory": "/path/to/output"
                }
            },
            {
                "type": "parquet",
                "description": "Parquet files",
                "config_example": {
                    "directory": "/path/to/output"
                }
            }
        ]
    }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
