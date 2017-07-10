package com.alibaba.middleware.race.sync.V2.db;

import com.alibaba.middleware.race.sync.Constants;
import com.alibaba.middleware.race.sync.Params;
import com.alibaba.middleware.race.sync.V2.utils.BytesUtil;
import com.alibaba.middleware.race.sync.V2.utils.TypeUtil;
import com.lmax.disruptor.EventFactory;

import java.nio.ByteBuffer;

/**
 * Created by xiyuanbupt on 6/23/17.
 */
public class BlockEvent{

    public final static EventFactory<BlockEvent> BLOCK_EVENT_EVENT_FACTORY = new EventFactory<BlockEvent>() {
        @Override
        public BlockEvent newInstance() {
            return new BlockEvent();
        }
    };

    enum UpdateType{
        PK, FIRST_NAME, LAST_NAME, SCORE
    }

    private BlockEvent(){
        byteBuffer = ByteBuffer.allocate(Params.BUFFER_SIZE_TABLE);
        maxRowNum = Params.MAX_ROW_PER_BUFFER;
        rowInfos = new RowInfo[maxRowNum];
        for(int i = 0; i<maxRowNum; i++){
            rowInfos[i] = new RowInfo();
        }
    }

    class RowInfo{
        byte op = 0;
        int id = 0;
        int idAft = 0;
        UpdateType updateType;
        boolean isMale;
        byte[] score = new byte[2];
        byte[] score2 = new byte[3];
        int fNameStart = 0;
        int fNameLen = 0;
        int lNameStart = 0;
        int lNameLen = 0;

        void mergeToTable(){
            // 此时需要保证id一定要小于big id
            if(op == Params.OP_UPDATE){
                switch (updateType){
                    case FIRST_NAME:
                        BigTable.INSTANCE.storeFirstName(data, fNameStart, fNameLen, id);
                        break;
                    case LAST_NAME:
                        BigTable.INSTANCE.storeLastName(data, lNameStart, lNameLen, id);
                        break;
                    case SCORE:
                        BigTable.INSTANCE.storeScore(score, id);
                        break;
                    case PK:
                        BitMap.INSTANCE.unset(id);
                        if(idAft < Params.BIG_TABLE_ROW_COUNT) {
                            BitMap.INSTANCE.set(idAft);
                            BigTable.INSTANCE.mv(id, idAft);
                        }
                        break;
                }
            }else if(op == Params.OP_INSERT){
                // first_name
                BigTable.INSTANCE.storeFirstNameFir(data, fNameStart, fNameLen, id);
                // last_name
                BigTable.INSTANCE.storeLastNameFir(data, lNameStart, lNameLen, id);
                // sex
                BigTable.INSTANCE.storeSex(isMale, id);
                // score
                BigTable.INSTANCE.storeScore(score, id);
                // score2
                BigTable.INSTANCE.storeScore2(score2, id);
                BitMap.INSTANCE.set(id);
            }else {
                BitMap.INSTANCE.unset(id);
            }
        }
    }

    void mergeToTable(){
        for(int i = 0; i<rowLen; i++){
            rowInfos[i].mergeToTable();
        }
    }

    public ByteBuffer byteBuffer;
    byte[] data;
    RowInfo[] rowInfos;
    int rowLen;
    final int maxRowNum;

    static final int skip = 54;

    public void parseBlock(){
        data = byteBuffer.array();
        int len = byteBuffer.limit();
        int pos = 0;
        byte op = 0;
        rowLen = 0;
        boolean first_sep;
        while (pos < len){
            RowInfo rowInfo = rowInfos[rowLen++];
            // TODO 可以删除pos参数
            int i = pos + skip;
            op = 0;
            first_sep = false;
            for(; op ==0; i++){
                if(data[i] == Params.SEP_COL){
                    if(!first_sep){
                        first_sep = true;
                    }else {
                        op = data[i - 1];
                    }
                }
            }
            rowInfo.op = op;
            if(op == Params.OP_UPDATE){
                i += 7;
                int start = i;
                while (data[i] != Params.SEP_COL)i++;
                rowInfo.id = BytesUtil.BytesIndexToInt(data, start, i-1);
                start = ++i;
                while (data[i] != Params.SEP_COL)i++;
                if(data[i+1] != Params.SEP_ROW){// 非主键变更
                    if(rowInfo.id < Params.BIG_TABLE_ROW_COUNT) {// 在范围之内的主键变更
                        int sep2 = 0, sep3 = 0;
                        switch (data[i + 7]) {
                            case 110: // 'n' |first_name:2:0|王|孙|
                                i += 16;
                                for (; sep3 == 0; i++) {
                                    if (data[i] == Params.SEP_COL) {
                                        if (sep2 == 0) {
                                            sep2 = i;
                                        } else {
                                            sep3 = i;
                                        }
                                    }
                                }
                                rowInfo.updateType = UpdateType.FIRST_NAME;
                                rowInfo.fNameStart = sep2 + 1;
                                rowInfo.fNameLen = sep3 - sep2 - 1;
                                pos = i + 1;
                                break;
                            case 97: // 'a'  |last_name:2:0|
                                i += 15;
                                for (; sep3 == 0; i++) {
                                    if (data[i] == Params.SEP_COL) {
                                        if (sep2 == 0) {
                                            sep2 = i;
                                        } else {
                                            sep3 = i;
                                        }
                                    }
                                }
                                rowInfo.updateType = UpdateType.LAST_NAME;
                                rowInfo.lNameStart = sep2 + 1;
                                rowInfo.lNameLen = sep3 - sep2 - 1;
                                pos = i + 1;
                                break;
                            case 49: // '1'  |score:1:0|100|200|
                                i += 11;
                                for (; sep3 == 0; i++) {
                                    if (data[i] == Params.SEP_COL) {
                                        if (sep2 == 0) {
                                            sep2 = i;
                                        } else {
                                            sep3 = i;
                                        }
                                    }
                                }
                                rowInfo.updateType = UpdateType.SCORE;
                                TypeUtil.write2byteInt(rowInfo.score, BytesUtil.BytesIndexToInt(
                                        data, sep2 + 1, sep3 - 1
                                ), 0);
                                pos = i + 1;
                                break;
                            default:
                                throw new RuntimeException("Don't match any");
                        }
                    }else { // rowInfo.id >= Parmas.BIG
                        rowLen --;
                        i += 18;
                        while (data[i] != Params.SEP_ROW)i++;
                        pos = i + 1;
                    }
                }else {// 主键变更
                    pos = i + 2;
                    if(rowInfo.id >= Params.BIG_TABLE_ROW_COUNT){
                        rowLen --;
                        continue;
                    }
                    int idAft = BytesUtil.BytesIndexToInt(data, start, i - 1);
                    rowInfo.updateType = UpdateType.PK;
                    rowInfo.idAft = idAft;
                }
            }else if(op == Params.OP_INSERT){ // |id:1:1|NULL|4231020|first_name:2:0|孙|郑|
                i += 12;
                int start = i;
                while (data[i] != Params.SEP_COL) i++;
                rowInfo.id = BytesUtil.BytesIndexToInt(data, start, i - 1);
                if(rowInfo.id < Params.BIG_TABLE_ROW_COUNT) {
                    i += 21; // 调到first_name 的第一个byte
                    start = rowInfo.fNameStart = i;
                    while (data[i] != Params.SEP_COL) {
                        i++;
                    }
                    rowInfo.fNameLen = i - start;
                    i += 20;
                    rowInfo.lNameStart = i;
                    while (data[i] != Params.SEP_COL) {
                        i++;
                    }
                    rowInfo.lNameLen = i - rowInfo.lNameStart;
                    i += 14;// sex first
                    rowInfo.isMale = (data[i]==Params.MALE_FIRST);
                    i += 19;
                    start = i;
                    while (data[i] != Params.SEP_COL) {
                        i++;
                    }
                    TypeUtil.write2byteInt(rowInfo.score, BytesUtil.BytesIndexToInt(
                            data, start, i - 1
                    ),0);

                    // TODO 测试环境加回来
                    // TODO 换回来
                    if (Constants.dev) {
                        pos = i + 2;
                    } else {
                        i += 17; // |797|score2:1:0|NULL|106271|
                        start = i;
                        while (data[i] != Params.SEP_COL) {
                            i++;
                        }
                        TypeUtil.write3byteInt(rowInfo.score2, BytesUtil.BytesIndexToInt(
                                data, start, i - 1
                        ), 0);
                        pos = i + 2;
                    }
                }else {
                    rowLen --;
                }
            }else { // delete
                i += 7;
                int start = i;
                while (data[i] != Params.SEP_COL)i++;
                rowInfo.id = BytesUtil.BytesIndexToInt(data, start, i-1);
                // TODO 线上环境和正式环境都要更改下
                //i += 95;
                i += 80;
                while (data[i] != Params.SEP_ROW)i++;
                pos = i + 1;
                if(rowInfo.id >= Params.BIG_TABLE_ROW_COUNT){
                    rowLen --;
                }
            }
        }
    }
}
