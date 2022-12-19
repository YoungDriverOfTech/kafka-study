package com.liang.kafkapricatice.questionnaire.common;

import java.util.UUID;
import lombok.Data;

/**
 * @description : 公共返回对象
 **/
@Data
public class BaseResponseVO<M> {

  private String requestId;
  private M result;

  public static<M> BaseResponseVO<M> success(){
    BaseResponseVO<M> baseResponseVO = new BaseResponseVO<>();
    baseResponseVO.setRequestId(genRequestId());

    return baseResponseVO;
  }

  public static<M> BaseResponseVO<M> success(M result){
    BaseResponseVO<M> baseResponseVO = new BaseResponseVO<>();
    baseResponseVO.setRequestId(genRequestId());
    baseResponseVO.setResult(result);

    return baseResponseVO;
  }

  private static String genRequestId(){
    return UUID.randomUUID().toString();
  }

}
