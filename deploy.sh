#!/bin/bash
# deploy.sh — git push 후 Railway 배포 완료까지 대기 + TTS 알림
set -e

SITE="https://web-production-a8d3a.up.railway.app"
VERSION_URL="$SITE/api/debug/version"

# 1) push 전 현재 commit
OLD_COMMIT=$(curl -s -H "User-Agent: SuperTagDeploy/1.0 Mozilla" "$VERSION_URL" 2>/dev/null | grep -o '"commit":"[^"]*"' | cut -d'"' -f4 || echo "none")
echo "[deploy] 현재 배포 commit: $OLD_COMMIT"

# 2) git push
echo "[deploy] git push origin master..."
git push origin master
NEW_COMMIT=$(git rev-parse HEAD | cut -c1-7)
echo "[deploy] 새 commit: $NEW_COMMIT"

if [ "$OLD_COMMIT" = "$NEW_COMMIT" ]; then
    echo "[deploy] 이미 최신 — 배포 불필요"
    exit 0
fi

# 3) 배포 완료 대기 (최대 3분)
echo "[deploy] Railway 빌드 대기 중..."
for i in $(seq 1 36); do
    sleep 5
    DEPLOYED=$(curl -s -H "User-Agent: SuperTagDeploy/1.0 Mozilla" --max-time 5 "$VERSION_URL" 2>/dev/null | grep -o '"commit":"[^"]*"' | cut -d'"' -f4 || echo "")
    if [ "$DEPLOYED" = "$NEW_COMMIT" ]; then
        echo "[deploy] 배포 완료! ($((i*5))초 소요)"
        # TTS 알림
        (sleep 5; powershell.exe 'Add-Type -AssemblyName System.Speech; $s = New-Object System.Speech.Synthesis.SpeechSynthesizer; $s.Volume = 100; $s.Rate = 0; [Console]::Beep(600,300); [Console]::Beep(800,300); [Console]::Beep(1000,500); Start-Sleep -Milliseconds 200; $s.Speak("레일웨이 배포 완료")') 2>/dev/null &
        exit 0
    fi
    printf "\r[deploy] 대기 중... %ds" $((i*5))
done

echo ""
echo "[deploy] 3분 경과 — 타임아웃. 수동 확인 필요"
(sleep 5; powershell.exe 'Add-Type -AssemblyName System.Speech; $s = New-Object System.Speech.Synthesis.SpeechSynthesizer; $s.Volume = 100; [Console]::Beep(400,800); $s.Speak("배포 타임아웃. 확인 필요합니다")') 2>/dev/null &
exit 1
