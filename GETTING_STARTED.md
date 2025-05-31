# 시작하기 - 빠른 참조

## ✅ 레포지토리 설정 완료!

바이낸스 퍼블릭 데이터 다운로더가 사용 준비 완료됨. 설정된 내용:

### 📁 생성/업데이트된 파일들:
- ✅ `requirements.txt` - 모든 의존성으로 업데이트됨 (`pandas`, `numpy`, `fgrequests`)
- ✅ `setup.sh` - 자동화된 설정 스크립트 (실행 가능)
- ✅ `README.md` - 메인 레포지토리 문서
- ✅ `python/README_SETUP.md` - 상세 사용 가이드
- ✅ `GETTING_STARTED.md` - 이 빠른 참조

### 🚀 바로 사용 가능한 명령어:

**1. 처음 설정 (한 번만 실행):**
```bash
cd binance-public-data
./setup.sh
```

**2. 데이터 다운로드:**
```bash
cd python

# 예제 1: 2024년 1월 비트코인 일봉 데이터 다운로드
python3 download-kline2.py -t spot -s BTCUSDT -i 1d -startDate 2024-01-01 -endDate 2024-01-31

# 예제 2: 여러 심볼에 시간봉 데이터 다운로드
python3 download-kline2.py -t spot -s BTCUSDT ETHUSDT ADAUSDT -i 1h -startDate 2024-01-01 -endDate 2024-01-07

# 예제 3: 커스텀 폴더에 다운로드
python3 download-kline2.py -t spot -s BTCUSDT -i 1d -startDate 2024-01-01 -folder ./my_data
```

### 📊 얻는 것:
- 정리된 폴더의 개별 CSV 파일들
- 병합된 파일: `SYMBOL_STARTDATE_ENDDATE.csv`
- 거래량 및 거래 수 데이터가 있는 표준 OHLCV 형식
- 중단된 다운로드에 대한 자동 재개 기능

### 🔧 작동하는 주요 기능들:
- ✅ 멀티 심볼 병렬 다운로드
- ✅ 자동 파일 병합
- ✅ 점진적 업데이트 (다운로드 재개)
- ✅ 데이터 검증 및 타임스탬프 보정
- ✅ 현물, 선물 (UM/CM) 거래 타입 지원
- ✅ 모든 간격: 1s, 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1mo

### 📖 문서:
- **빠른 시작**: `README.md`
- **상세 가이드**: `python/README_SETUP.md`
- **스크립트 참조**: `python/download-kline2.py` (주석 잘 되어 있음)

### ✅ 테스트 확인됨:
다음으로 설정이 성공적으로 테스트됨:
```bash
python3 download-kline2.py -t spot -s BTCUSDT -i 1d -startDate 2024-01-01 -endDate 2024-01-02
```
- 월간 및 일간 데이터 다운로드
- CSV 파일 추출 및 병합
- 적절한 디렉토리 구조 생성
- 최종 병합 파일 생성: `BTCUSDT_20240101_20240131.csv`

**🎉 모든 준비 완료! 이제 암호화폐 데이터 다운로드 시작하기!**