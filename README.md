# 바이낸스 퍼블릭 데이터 다운로더

바이낸스 퍼블릭 데이터 저장소에서 과거 암호화폐 데이터를 다운로드하는 파이썬 툴킷임.

## 🚀 빠른 시작

### 1. 설정
```bash
# 레포지토리로 이동
cd binance-public-data

# 설정 스크립트 실행 (의존성 설치)
./setup.sh
```

### 2. 데이터 다운로드
```bash
cd python

# 비트코인 일봉 데이터 다운로드 (지난 달)
python3 download-kline2.py -t spot -s BTCUSDT -i 1d -startDate 2024-01-01 -endDate 2024-01-31

# 여러 심볼에 다양한 간격으로 다운로드
python3 download-kline2.py -t spot -s BTCUSDT ETHUSDT -i 1h 1d -startDate 2024-01-01

질문1, 2 데이터 다운로드
python ./binance-public-data/python/download-kline2.py -t spot -i 1d -skip-daily 1 -s btcusdt ltcusdt neousdt ethusdt
usdt로 다운로드 한 이유는 usdc는 데이터가 더 짧음

```

## 📋 기능

- ✅ **멀티 심볼 다운로드**: 여러 암호화폐를 동시에 다운로드함
- ✅ **유연한 간격**: 모든 바이낸스 간격 지원 (1초 ~ 1개월)
- ✅ **자동 병합**: 개별 파일들을 종합 데이터셋으로 자동 병합함
- ✅ **점진적 업데이트**: 중단된 다운로드 재개 및 기존 데이터 업데이트
- ✅ **비동기 다운로드**: `fgrequests`를 사용한 빠른 병렬 다운로드
- ✅ **데이터 검증**: 자동 타임스탬프 보정 및 데이터 일관성 검사
- ✅ **다양한 거래 타입**: 현물, USD-M 선물, COIN-M 선물 지원

## 📁 프로젝트 구조

```
binance-public-data/
├── python/
│   ├── download-kline2.py      # 메인 다운로더 스크립트
│   ├── enums.py                # 상수 및 열거형
│   ├── utility.py              # 헬퍼 함수들
│   ├── requirements.txt        # 파이썬 의존성
│   └── README_SETUP.md         # 상세 사용 가이드
├── setup.sh                    # 빠른 설정 스크립트
└── README.md                   # 이 파일
```

## 🛠 요구사항

- Python 3.7+
- 의존성: `pandas`, `numpy`, `fgrequests`

## 📖 문서

상세한 사용법, 명령줄 옵션, 예제는 여기 참고:
- [`python/README_SETUP.md`](python/README_SETUP.md) - 완전한 사용 가이드
- [`python/download-kline2.py`](python/download-kline2.py) - 인라인 문서가 있는 메인 스크립트

## 🎯 일반적인 사용 사례

### 분석용 과거 데이터 다운로드
```bash
# 주요 코인들의 1년치 일봉 OHLCV 데이터 가져오기
python3 download-kline2.py -t spot -s BTCUSDT ETHUSDT ADAUSDT -i 1d -startDate 2023-01-01 -endDate 2023-12-31
```

### 고빈도 거래 데이터
```bash
# 알고리즘 거래용 분봉 데이터 가져오기
python3 download-kline2.py -t spot -s BTCUSDT -i 1m -startDate 2024-01-01 -endDate 2024-01-07
```

### 멀티 타임프레임 분석
```bash
# 기술적 분석을 위한 여러 간격 다운로드
python3 download-kline2.py -t spot -s ETHUSDT -i 1h 4h 1d -startDate 2024-01-01
```

## 📊 출력 형식

데이터는 다음 구조의 CSV 파일로 저장됨:
```
open_time,open,high,low,close,volume,close_time,quote_volume,count,taker_buy_volume,taker_buy_quote_volume,ignore
1640995200000,46222.99,46929.00,46089.00,46904.99,1234.56789,1641081599999,57234567.89,12345,567.89123,26234567.89,0
```

병합된 파일 이름: `SYMBOL_STARTDATE_ENDDATE.csv` (예: `BTCUSDT_20240101_20241231.csv`)

## 🚨 중요 사항

- **요청 제한**: 스크립트는 바이낸스의 퍼블릭 데이터 제한을 존중함
- **점진적 업데이트**: 스크립트가 기존 데이터를 자동 감지해서 재다운로드를 피함
