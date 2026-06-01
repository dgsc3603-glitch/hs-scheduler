# -*- coding: utf-8 -*-
"""
텔레그램 원격 조작 모듈 (스케쥴러 내장형)
- 한국어 명령어 100% 지원
- 인라인 키보드 버튼으로 터치 한 번에 조작
- 스케쥴러 코어와 완전 통합
- 별도 스레드에서 비동기 Long Polling 실행
"""

import threading
import asyncio
import time
import datetime
import os
import sys
import json

# httpx가 없을 경우 urllib 폴백
try:
    import httpx
    HAS_HTTPX = True
except ImportError:
    HAS_HTTPX = False
    import urllib.request
    import urllib.parse


class SchedulerTelegramBot:
    """스케쥴러 코어와 통합된 텔레그램 봇"""
    
    def __init__(self, scheduler_core, credentials, ui_callback=None):
        self.core = scheduler_core
        self.credentials = credentials
        self.ui_callback = ui_callback  # MainWindow.update_bot_status
        
        self._token = None
        self._chat_id = None
        self.running = False
        self._thread = None
        self._offset = None
        
        self._load_credentials()
    
    DEFAULT_TOKEN = ""
    DEFAULT_CHAT_ID = ""
    
    def _load_credentials(self):
        try:
            secrets = self.credentials.load()
            self._token = secrets.get("TELEGRAM_BOT_TOKEN") or self.DEFAULT_TOKEN
            self._chat_id = secrets.get("CHAT_ID") or self.DEFAULT_CHAT_ID
        except:
            self._token = self.DEFAULT_TOKEN
            self._chat_id = self.DEFAULT_CHAT_ID
    
    @property
    def is_configured(self):
        return bool(self._token and self._chat_id)
    
    def start(self):
        if not self.is_configured:
            self.core.log("📡 텔레그램 봇: 토큰 미설정 (scheduler_secrets.json 확인)")
            return False
        
        self.running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True, name="TelegramBot")
        self._thread.start()
        self.core.log("📡 텔레그램 봇: 시작됨")
        return True
    
    def stop(self):
        self.running = False
    
    def _run_loop(self):
        if HAS_HTTPX:
            asyncio.run(self._async_polling_loop())
        else:
            self._sync_polling_loop()
    
    # ============================================================
    # 비동기 폴링 (httpx)
    # ============================================================
    async def _async_polling_loop(self):
        async with httpx.AsyncClient(timeout=35) as client:
            # 기존 webhook/세션 초기화 (409 방지)
            try:
                await client.post(
                    f"https://api.telegram.org/bot{self._token}/deleteWebhook",
                    json={"drop_pending_updates": False}
                )
            except:
                pass
            
            await self._send_message_async(client, "🤖 스케쥴러 봇 준비 완료!\n\n'도움말' 또는 '메뉴' 를 입력하세요.")
            if self.ui_callback:
                try: self.ui_callback(connected=True)
                except: pass
            
            while self.running:
                try:
                    url = f"https://api.telegram.org/bot{self._token}/getUpdates?timeout=30"
                    if self._offset:
                        url += f"&offset={self._offset + 1}"
                    
                    res = await client.get(url)
                    res.raise_for_status()
                    updates = res.json()
                    
                    for update in updates.get("result", []):
                        self._offset = update["update_id"]
                        
                        # 콜백 쿼리 (인라인 키보드 버튼 클릭)
                        cb = update.get("callback_query")
                        if cb:
                            await self._answer_callback_async(client, cb["id"])
                            await self._handle_callback_async(client, cb["data"])
                            continue
                        
                        # 텍스트 메시지
                        msg_text = update.get("message", {}).get("text", "").strip()
                        if msg_text:
                            await self._handle_command_async(client, msg_text)
                
                except httpx.ReadTimeout:
                    pass
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 409:
                        self.core.log("📡 텔레그램: 다른 봇 인스턴스 감지 (409). 15초 후 재시도...")
                        await asyncio.sleep(15)
                    else:
                        self.core.log(f"📡 텔레그램 폴링 오류: {e}")
                        await asyncio.sleep(5)
                except Exception as e:
                    self.core.log(f"📡 텔레그램 폴링 오류: {e}")
                    await asyncio.sleep(5)
    
    async def _send_message_async(self, client, text, reply_markup=None):
        url = f"https://api.telegram.org/bot{self._token}/sendMessage"
        payload = {"chat_id": self._chat_id, "text": text, "parse_mode": "HTML"}
        if reply_markup:
            payload["reply_markup"] = json.dumps(reply_markup)
        try:
            await client.post(url, json=payload, timeout=10)
        except Exception as e:
            self.core.log(f"📡 텔레그램 전송 실패: {e}")
    
    async def _answer_callback_async(self, client, callback_id):
        try:
            url = f"https://api.telegram.org/bot{self._token}/answerCallbackQuery"
            await client.post(url, json={"callback_query_id": callback_id}, timeout=5)
        except: pass
    
    async def _handle_command_async(self, client, text):
        text_lower = text.lower().strip()
        
        # 한국어 + 영어 명령어 파싱
        if text_lower in ["도움말", "/help", "?"]:
            await self._cmd_help(client)
        elif text_lower in ["상태", "/status", "현황"]:
            await self._cmd_status(client)
        elif text_lower in ["실행", "/run", "메뉴", "시작"]:
            await self._cmd_run_menu(client)
        elif text_lower in ["중지", "/stop", "멈춰", "스톱"]:
            await self._cmd_stop(client)
        elif text_lower in ["로그", "/log", "최근로그"]:
            await self._cmd_log(client)
        elif text_lower in ["다음", "/next", "예정", "스케줄"]:
            await self._cmd_next(client)
        elif text_lower in ["메모리", "/mem", "ram", "리소스"]:
            await self._cmd_memory(client)
        elif text_lower.startswith("일시정지 ") or text_lower.startswith("/pause "):
            name = text.split(" ", 1)[1].strip()
            await self._cmd_pause(client, name)
        elif text_lower.startswith("재개 ") or text_lower.startswith("/resume "):
            name = text.split(" ", 1)[1].strip()
            await self._cmd_resume(client, name)
        elif text_lower.startswith("실행 ") or text_lower.startswith("/run "):
            name = text.split(" ", 1)[1].strip()
            await self._cmd_run_project(client, name)
        else:
            await self._send_message_async(client, 
                f"❓ 알 수 없는 명령어입니다.\n\n'도움말' 을 입력하면 사용 가능한 명령어를 볼 수 있어요.")
    
    async def _handle_callback_async(self, client, data):
        if data.startswith("run:"):
            proj_name = data.split(":", 1)[1]
            await self._cmd_run_project(client, proj_name)
        elif data.startswith("stop:"):
            proj_name = data.split(":", 1)[1]
            await self._cmd_stop_project(client, proj_name)
        elif data.startswith("pause:"):
            proj_name = data.split(":", 1)[1]
            await self._cmd_pause(client, proj_name)
        elif data.startswith("resume:"):
            proj_name = data.split(":", 1)[1]
            await self._cmd_resume(client, proj_name)
        elif data == "status":
            await self._cmd_status(client)
        elif data == "log":
            await self._cmd_log(client)
        elif data == "mem":
            await self._cmd_memory(client)
        elif data == "next":
            await self._cmd_next(client)
    
    # ============================================================
    # 한국어 명령어 핸들러
    # ============================================================
    async def _cmd_help(self, client):
        help_text = (
            "📖 <b>스케쥴러 봇 명령어</b>\n"
            "━━━━━━━━━━━━━━━━━━\n"
            "\n"
            "📋 <b>조회 명령</b>\n"
            "  <b>상태</b> — 전체 프로젝트 현황\n"
            "  <b>다음</b> — 다음 실행 예정 시간\n"
            "  <b>로그</b> — 최근 실행 로그\n"
            "  <b>메모리</b> — RAM / 시스템 정보\n"
            "\n"
            "🎮 <b>조작 명령</b>\n"
            "  <b>실행</b> — 프로젝트 선택 메뉴 표시\n"
            "  <b>실행 [이름]</b> — 특정 프로젝트 즉시 실행\n"
            "  <b>중지</b> — 실행 중인 프로젝트 중지\n"
            "  <b>일시정지 [이름]</b> — 프로젝트 비활성화\n"
            "  <b>재개 [이름]</b> — 프로젝트 활성화\n"
            "\n"
            "💡 버튼을 눌러도 동일하게 동작합니다!"
        )
        
        keyboard = {"inline_keyboard": [
            [{"text": "📊 상태", "callback_data": "status"}, {"text": "📅 다음", "callback_data": "next"}],
            [{"text": "📄 로그", "callback_data": "log"}, {"text": "💾 메모리", "callback_data": "mem"}],
        ]}
        await self._send_message_async(client, help_text, reply_markup=keyboard)
    
    async def _cmd_status(self, client):
        projects = self.core.projects
        if not projects:
            await self._send_message_async(client, "📭 등록된 프로젝트가 없습니다.")
            return
        
        lines = ["📊 <b>프로젝트 상태</b>\n━━━━━━━━━━━━━━━"]
        for p in projects:
            icon = "⚪"
            if p.status == self.core.STATUS_RUNNING: icon = "🟢"
            elif p.status == self.core.STATUS_COMPLETED: icon = "🔵"
            elif "오류" in p.status: icon = "🔴"
            elif not p.enabled: icon = "⏸"
            
            enabled = "✅" if p.enabled else "⏸"
            progress = ""
            if p.total_tasks > 0 and p.completed_tasks > 0:
                pct = int(p.completed_tasks / p.total_tasks * 100)
                progress = f" [{pct}%]"
            
            lines.append(f"{icon} <b>{p.name}</b> {enabled}")
            lines.append(f"   상태: {p.status}{progress}")
            lines.append(f"   다음: {p.next_run}")
            lines.append("")
        
        await self._send_message_async(client, "\n".join(lines))
    
    async def _cmd_run_menu(self, client):
        projects = self.core.projects
        if not projects:
            await self._send_message_async(client, "📭 등록된 프로젝트가 없습니다.")
            return
        
        keyboard_rows = []
        for p in projects:
            icon = "▶" if p.enabled else "⏸"
            status = "🟢" if p.status == self.core.STATUS_RUNNING else ""
            keyboard_rows.append([
                {"text": f"{icon} {p.name} {status}", "callback_data": f"run:{p.name}"}
            ])
        
        # 하단 유틸 버튼
        keyboard_rows.append([
            {"text": "📊 상태", "callback_data": "status"},
            {"text": "⏹ 중지", "callback_data": "stop:__all__"},
        ])
        
        keyboard = {"inline_keyboard": keyboard_rows}
        await self._send_message_async(client, "🎮 실행할 프로젝트를 선택하세요:", reply_markup=keyboard)
    
    async def _cmd_run_project(self, client, name):
        proj = self._find_project(name)
        if not proj:
            await self._send_message_async(client, f"❌ '{name}' 프로젝트를 찾을 수 없습니다.")
            return
        
        if proj.status == self.core.STATUS_RUNNING:
            await self._send_message_async(client, f"⚠️ [{proj.name}] 이미 실행 중입니다.")
            return
        
        success = self.core.run_project_manual(proj)
        if success:
            await self._send_message_async(client, f"▶️ <b>[{proj.name}]</b> 실행 시작!")
        else:
            await self._send_message_async(client, f"⚠️ [{proj.name}] 실행 시작 실패 (동시 실행 한도 또는 이미 실행 중)")
    
    async def _cmd_stop(self, client):
        running = [p for p in self.core.projects if p.status == self.core.STATUS_RUNNING]
        if not running:
            await self._send_message_async(client, "📭 현재 실행 중인 프로젝트가 없습니다.")
            return
        
        if len(running) == 1:
            await self._cmd_stop_project(client, running[0].name)
        else:
            keyboard_rows = []
            for p in running:
                keyboard_rows.append([
                    {"text": f"⏹ {p.name} 중지", "callback_data": f"stop:{p.name}"}
                ])
            keyboard = {"inline_keyboard": keyboard_rows}
            await self._send_message_async(client, "⏹ 중지할 프로젝트를 선택하세요:", reply_markup=keyboard)
    
    async def _cmd_stop_project(self, client, name):
        if name == "__all__":
            running = [p for p in self.core.projects if p.status == self.core.STATUS_RUNNING]
            for p in running:
                p.stop_requested = True
                self.core.stop_project(p.name)
            await self._send_message_async(client, f"⏹ 실행 중인 {len(running)}개 프로젝트에 중지 명령 전송")
            return
        
        proj = self._find_project(name)
        if not proj:
            await self._send_message_async(client, f"❌ '{name}' 프로젝트를 찾을 수 없습니다.")
            return
        
        proj.stop_requested = True
        self.core.stop_project(proj.name)
        await self._send_message_async(client, f"⏹ <b>[{proj.name}]</b> 중지 명령 전송 완료")
    
    async def _cmd_log(self, client):
        # 가장 최근 실행된 프로젝트의 로그 파일 찾기
        log_base = os.path.join(os.path.dirname(self.core.data_file), "task_logs")
        if not os.path.exists(log_base):
            await self._send_message_async(client, "📭 로그 파일이 없습니다.")
            return
        
        # 최신 로그 파일 찾기
        latest_file = None
        latest_time = 0
        for root, dirs, files in os.walk(log_base):
            for f in files:
                if f.endswith(".txt") and not f.startswith("_temp"):
                    fpath = os.path.join(root, f)
                    mtime = os.path.getmtime(fpath)
                    if mtime > latest_time:
                        latest_time = mtime
                        latest_file = fpath
        
        if not latest_file:
            await self._send_message_async(client, "📭 로그 파일이 없습니다.")
            return
        
        try:
            with open(latest_file, "r", encoding="utf-8", errors="ignore") as f:
                content = f.read()[-1500:]  # 마지막 1500자
            
            fname = os.path.basename(latest_file)
            time_str = datetime.datetime.fromtimestamp(latest_time).strftime("%Y-%m-%d %H:%M:%S")
            await self._send_message_async(client, f"📄 <b>최근 로그</b> ({fname})\n⏰ {time_str}\n━━━━━━━━━━━━\n{content}")
        except Exception as e:
            await self._send_message_async(client, f"⚠️ 로그 읽기 실패: {e}")
    
    async def _cmd_next(self, client):
        projects = self.core.projects
        if not projects:
            await self._send_message_async(client, "📭 등록된 프로젝트가 없습니다.")
            return
        
        lines = ["📅 <b>다음 실행 예정</b>\n━━━━━━━━━━━━━━━"]
        for p in sorted(projects, key=lambda x: x.next_run if x.next_run not in ["-", "일시중지", "기간만료", "설정필요", "설정오류", "오류", "형식오류"] else "9999"):
            icon = "⏸" if not p.enabled else "📌"
            lines.append(f"{icon} <b>{p.name}</b>")
            lines.append(f"   ⏰ {p.next_run}")
            lines.append("")
        
        await self._send_message_async(client, "\n".join(lines))
    
    async def _cmd_pause(self, client, name):
        proj = self._find_project(name)
        if not proj:
            await self._send_message_async(client, f"❌ '{name}' 프로젝트를 찾을 수 없습니다.")
            return
        
        proj.enabled = False
        proj.calculate_next_run()
        self.core.save_data()
        await self._send_message_async(client, f"⏸ <b>[{proj.name}]</b> 일시정지 완료")
    
    async def _cmd_resume(self, client, name):
        proj = self._find_project(name)
        if not proj:
            await self._send_message_async(client, f"❌ '{name}' 프로젝트를 찾을 수 없습니다.")
            return
        
        proj.enabled = True
        proj.calculate_next_run()
        self.core.save_data()
        await self._send_message_async(client, f"▶️ <b>[{proj.name}]</b> 재개 완료\n⏰ 다음 실행: {proj.next_run}")
    
    async def _cmd_memory(self, client):
        lines = ["💾 <b>시스템 리소스</b>\n━━━━━━━━━━━━━━━"]
        try:
            import psutil
            proc = psutil.Process()
            mem = proc.memory_info()
            lines.append(f"  스케쥴러 RAM: {mem.rss / 1024 / 1024:.1f} MB")
            lines.append(f"  스레드 수: {proc.num_threads()}")
            
            sys_mem = psutil.virtual_memory()
            lines.append(f"\n  시스템 전체: {sys_mem.total / 1024**3:.1f} GB")
            lines.append(f"  사용 중: {sys_mem.used / 1024**3:.1f} GB ({sys_mem.percent}%)")
            lines.append(f"  가용: {sys_mem.available / 1024**3:.1f} GB")
            
            cpu = psutil.cpu_percent(interval=1)
            lines.append(f"\n  CPU 사용률: {cpu}%")
        except ImportError:
            lines.append("  ⚠️ psutil 미설치 (pip install psutil)")
        except Exception as e:
            lines.append(f"  ⚠️ 정보 수집 오류: {e}")
        
        # 활성 프로세스 수
        with self.core.active_processes_lock:
            active_count = len(self.core.active_processes)
        lines.append(f"\n  활성 프로세스: {active_count}개")
        lines.append(f"  등록 프로젝트: {len(self.core.projects)}개")
        
        await self._send_message_async(client, "\n".join(lines))
    
    # ============================================================
    # 유틸리티
    # ============================================================
    def _find_project(self, name):
        """이름으로 프로젝트 찾기 (부분 매칭 지원)"""
        # 정확히 일치
        for p in self.core.projects:
            if p.name == name:
                return p
        # 부분 매칭
        for p in self.core.projects:
            if name.lower() in p.name.lower():
                return p
        return None
    
    def send_alert(self, message):
        """외부에서 호출 가능한 알림 전송 (동기식 폴백)"""
        if not self.is_configured:
            return
        
        if HAS_HTTPX:
            try:
                with httpx.Client(timeout=10) as client:
                    url = f"https://api.telegram.org/bot{self._token}/sendMessage"
                    client.post(url, json={"chat_id": self._chat_id, "text": message})
            except: pass
        else:
            try:
                url = f"https://api.telegram.org/bot{self._token}/sendMessage"
                data = urllib.parse.urlencode({"chat_id": self._chat_id, "text": message}).encode()
                req = urllib.request.Request(url, data=data)
                with urllib.request.urlopen(req, timeout=10) as resp:
                    pass
            except: pass
    
    # ============================================================
    # 동기 폴링 폴백 (httpx 미설치 시)
    # ============================================================
    def _sync_polling_loop(self):
        self.send_alert("🤖 스케쥴러 봇 준비 완료! (동기 모드)\n\n'도움말' 을 입력하세요.")
        if self.ui_callback:
            try: self.ui_callback(connected=True)
            except: pass
        
        while self.running:
            try:
                url = f"https://api.telegram.org/bot{self._token}/getUpdates?timeout=30"
                if self._offset:
                    url += f"&offset={self._offset + 1}"
                
                req = urllib.request.Request(url)
                with urllib.request.urlopen(req, timeout=35) as resp:
                    updates = json.loads(resp.read().decode())
                
                for update in updates.get("result", []):
                    self._offset = update["update_id"]
                    msg_text = update.get("message", {}).get("text", "").strip()
                    
                    if msg_text:
                        self._handle_sync_command(msg_text)
            except Exception as e:
                if "timed out" not in str(e).lower():
                    self.core.log(f"📡 텔레그램 폴링 오류: {e}")
                time.sleep(2)
    
    def _handle_sync_command(self, text):
        """동기 모드에서의 간단한 명령어 처리"""
        text_lower = text.lower().strip()
        
        if text_lower in ["도움말", "/help", "?"]:
            self.send_alert(
                "📖 스케쥴러 봇 명령어\n"
                "━━━━━━━━━━━━\n"
                "상태 — 프로젝트 현황\n"
                "다음 — 다음 실행 시간\n"
                "실행 [이름] — 프로젝트 실행\n"
                "중지 — 실행 중 중지\n"
                "메모리 — RAM 상태\n"
                "도움말 — 이 안내"
            )
        elif text_lower in ["상태", "/status"]:
            lines = ["📊 프로젝트 상태\n━━━━━━━━"]
            for p in self.core.projects:
                icon = "⚪"
                if p.status == self.core.STATUS_RUNNING: icon = "🟢"
                elif "오류" in p.status: icon = "🔴"
                elif not p.enabled: icon = "⏸"
                lines.append(f"{icon} {p.name}: {p.status}")
            self.send_alert("\n".join(lines))
        elif text_lower in ["메모리", "/mem"]:
            try:
                import psutil
                mem = psutil.Process().memory_info()
                self.send_alert(f"💾 RAM: {mem.rss/1024/1024:.0f} MB")
            except:
                self.send_alert("💾 psutil 미설치")
