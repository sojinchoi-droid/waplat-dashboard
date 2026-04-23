# 와플랫 대시보드 Watchdog — 서버 다운 시 자동 재시작
# 30초마다 python 프로세스 확인, 없으면 즉시 재기동

$AppDir  = "C:\Users\NHN\_bmad-output\dashboard-app"
$Python  = "C:\Python314\python.exe"
$LogFile = "$AppDir\watchdog.log"

function Write-Log($msg) {
    $line = "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')  $msg"
    # 로그 2MB 초과 시 초기화
    if ((Test-Path $LogFile) -and ((Get-Item $LogFile).Length -gt 2MB)) {
        Set-Content $LogFile "[로그 초기화]" -Encoding UTF8
    }
    Add-Content $LogFile $line -Encoding UTF8
}

function Start-Dashboard {
    & $Python -m streamlit run "$AppDir\app.py" `
        --server.port 8501 --server.headless true `
        *>> "$AppDir\streamlit.log" &
    Start-Sleep -Seconds 6
}

Write-Log "=== Watchdog 시작 ==="

# 최초: 기존 프로세스 정리 후 기동
Stop-Process -Name python -Force -ErrorAction SilentlyContinue
Start-Sleep -Milliseconds 800
Start-Dashboard
Write-Log "서버 최초 기동 완료"

$failCount = 0

while ($true) {
    Start-Sleep -Seconds 30

    if (-not (Get-Process -Name python -ErrorAction SilentlyContinue)) {
        $failCount++
        Write-Log "서버 다운 감지 (${failCount}회) — 재시작"
        Start-Dashboard
        Write-Log "재시작 완료"
        if ($failCount -ge 5) {
            Write-Log "경고: 5회 연속 다운. 앱 오류 확인 필요."
            $failCount = 0
        }
    } else {
        $failCount = 0
    }
}
