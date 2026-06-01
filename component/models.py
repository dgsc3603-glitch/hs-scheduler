import os
import uuid
import datetime
import threading

class ProjectTask:
    def __init__(self, filepath, step, args="", timeout=0, max_retries=0, order=0, task_id=None):
        self.task_id = task_id if task_id else str(uuid.uuid4())
        self.filepath = filepath
        self.filename = os.path.basename(filepath)
        self.step = int(step)
        self.args = args
        self.timeout = int(timeout)
        self.max_retries = int(max_retries)
        self.order = int(order)
        self.status = "대기"
        self.checked = False
        self.condition = {
            "enabled": False,
            "type": "file_exists",
            "value": ""
        }

    def to_dict(self):
        return {
            "task_id": self.task_id,
            "filepath": self.filepath, 
            "step": self.step,
            "args": self.args,
            "timeout": self.timeout,
            "max_retries": self.max_retries,
            "order": self.order,
            "status": self.status,
            "checked": self.checked,
            "condition": getattr(self, 'condition', {"enabled": False, "type": "file_exists", "value": ""})
        }

class Project:
    def __init__(self, name, run_time, tasks_data=None, schedule_type="daily", schedule_value="", dependencies=None, enabled=True, step_mode="parallel", **kwargs):
        self.name = name
        self.run_time = run_time
        self.schedule_type = schedule_type
        self.schedule_value = schedule_value
        self.dependencies = dependencies if dependencies else []
        self.enabled = enabled
        self.step_mode = step_mode
        
        self.tasks = []
        self.status = kwargs.get("status", "대기중")
        self.last_run = kwargs.get("last_run", "-")
        self.next_run = "-"
        
        self.total_tasks = kwargs.get("total_tasks", 0)
        self.completed_tasks = kwargs.get("completed_tasks", 0)
        self.last_checkpoint = kwargs.get("last_checkpoint", {
            "failed_step": None,
            "failed_task": None,
            "timestamp": None
        })
        
        l_exec_time = kwargs.get("last_execution_time")
        if isinstance(l_exec_time, str):
            try: self.last_execution_time = datetime.datetime.fromisoformat(l_exec_time)
            except: self.last_execution_time = None
        else: self.last_execution_time = l_exec_time

        self.last_executed_minute = kwargs.get("last_executed_minute")
        self.last_consumed_ticket = kwargs.get("last_consumed_ticket")
        self.execution_id = kwargs.get("execution_id", 0)
        self.catch_up_missed = kwargs.get("catch_up_missed", False)
        self.last_trigger_source = kwargs.get("last_trigger_source", "-")
        self.last_manual_run = kwargs.get("last_manual_run", "-")
        self.last_scheduled_run = kwargs.get("last_scheduled_run", "-")
        # This lock is only used to prevent duplicate project launches.
        # A plain Lock is safer here because stale locks can be released
        # by recovery code after a crashed worker thread.
        self.execution_lock = threading.Lock()
        self.stop_requested = kwargs.get("stop_requested", False)
        
        if tasks_data:
            for i, t_data in enumerate(tasks_data):
                task = ProjectTask(
                    t_data["filepath"], 
                    t_data["step"],
                    t_data.get("args", ""),
                    t_data.get("timeout", 0),
                    t_data.get("max_retries", 0),
                    t_data.get("order", i),
                    t_data.get("task_id")
                )
                task.checked = t_data.get("checked", False)
                task.status = t_data.get("status", "대기")
                task.condition = t_data.get("condition", {"enabled": False, "type": "file_exists", "value": ""})
                self.tasks.append(task)
            self.tasks.sort(key=lambda x: (x.step, x.order))
            
        # If status was completed today, preserve it. Else reset to waiting if it's a new day.
        today_str = datetime.datetime.now().strftime("%Y-%m-%d")
        if self.last_run and self.last_run.startswith(today_str):
            pass # Keep saved status (could be Completed or Error)
        else:
            self.status = "대기중"
            for t in self.tasks: t.status = "대기"
            
        self.calculate_next_run()

    def add_task(self, filepath, step):
        same_step_tasks = [t for t in self.tasks if t.step == step]
        max_order = max((t.order for t in same_step_tasks), default=-1)
        task = ProjectTask(filepath, step, order=max_order + 1)
        self.tasks.append(task)
        self.tasks.sort(key=lambda x: (x.step, x.order))

    def get_tasks_by_step(self):
        steps = {}
        for task in self.tasks:
            if task.step not in steps: steps[task.step] = []
            steps[task.step].append(task)
        return dict(sorted(steps.items()))

    def to_dict(self):
        return {
            "name": self.name,
            "run_time": self.run_time,
            "schedule_type": self.schedule_type,
            "schedule_value": self.schedule_value,
            "dependencies": self.dependencies,
            "enabled": self.enabled,
            "step_mode": self.step_mode,
            "tasks": [t.to_dict() for t in self.tasks],
            "status": self.status,
            "total_tasks": self.total_tasks,
            "completed_tasks": self.completed_tasks,
            "last_run": self.last_run,
            "last_execution_time": self.last_execution_time.isoformat() if self.last_execution_time else None,
            "last_executed_minute": self.last_executed_minute,
            "stop_requested": self.stop_requested,
            "last_consumed_ticket": self.last_consumed_ticket,
            "execution_id": self.execution_id,
            "catch_up_missed": self.catch_up_missed,
            "last_trigger_source": self.last_trigger_source,
            "last_manual_run": self.last_manual_run,
            "last_scheduled_run": self.last_scheduled_run
        }

    def calculate_next_run(self):
        now = datetime.datetime.now()
        if not self.enabled:
            self.next_run = "일시중지"
            return

        try:
            if self.schedule_type == "daily":
                target_time = datetime.datetime.strptime(self.run_time, "%H:%M").time()
                today_run = datetime.datetime.combine(now.date(), target_time)
                today_ticket = today_run.strftime("%Y-%m-%d %H:%M")

                if today_run <= now:
                    # 오늘 예정 시간이 이미 지남 → 티켓 소비 여부로 판단
                    if self.last_consumed_ticket == today_ticket:
                        # 오늘 티켓 이미 소비됨 → 내일로 설정
                        next_run = today_run + datetime.timedelta(days=1)
                    else:
                        # 오늘 티켓 미소비 (재시작으로 놓쳤을 가능성) → 오늘로 유지
                        next_run = today_run
                else:
                    next_run = today_run

                self.next_run = next_run.strftime("%Y-%m-%d %H:%M")
                
            elif self.schedule_type == "weekly":
                if not self.schedule_value: 
                    self.next_run = "설정필요"
                    return
                
                target_days = [d.strip()[:3].lower() for d in self.schedule_value.split(',')]
                weekdays = ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun']
                target_time = datetime.datetime.strptime(self.run_time, "%H:%M").time()
                
                candidates = []
                for day_str in target_days:
                    if day_str not in weekdays: continue
                    target_idx = weekdays.index(day_str)
                    current_idx = now.weekday()
                    days_ahead = target_idx - current_idx
                    if days_ahead < 0: days_ahead += 7
                    
                    candidate = datetime.datetime.combine(now.date() + datetime.timedelta(days=days_ahead), target_time)
                    if candidate <= now:
                        candidate += datetime.timedelta(days=7)
                    candidates.append(candidate)
                
                if candidates:
                    self.next_run = min(candidates).strftime("%Y-%m-%d %H:%M")
                else:
                    self.next_run = "설정오류"

            elif self.schedule_type == "interval":
                if not self.schedule_value.isdigit():
                    self.next_run = "설정오류"
                    return
                interval_min = int(self.schedule_value)
                if self.last_run == "-" or self.last_run == "실패":
                    self.next_run = now.strftime("%Y-%m-%d %H:%M")
                else:
                    last_run_dt = datetime.datetime.strptime(self.last_run, "%Y-%m-%d %H:%M")
                    next_run = last_run_dt + datetime.timedelta(minutes=interval_min)
                    if next_run <= now:
                         self.next_run = now.strftime("%Y-%m-%d %H:%M")
                    else:
                        self.next_run = next_run.strftime("%Y-%m-%d %H:%M")

            elif self.schedule_type == "onetime":
                try:
                    target_dt = datetime.datetime.strptime(self.schedule_value, "%Y-%m-%d %H:%M")
                    target_ticket = target_dt.strftime("%Y-%m-%d %H:%M")
                    if target_dt <= now and self.last_consumed_ticket == target_ticket:
                        self.next_run = "기간만료"
                    else:
                        self.next_run = target_ticket
                except:
                    self.next_run = "형식오류"
            else:
                self.next_run = "-"
        except Exception as e:
            print(f"Calc Next Run Error: {e}")
            self.next_run = "오류"
