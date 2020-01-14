package code.ponfee.es.uss.res;

import java.beans.Transient;
import java.io.Serializable;
import java.util.Date;
import java.util.Optional;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;

import code.ponfee.commons.ws.adapter.MarshalJsonResult;

/**
 * USS base result
 * 
 * @author Ponfee
 */
public class BaseResult implements Serializable, MarshalJsonResult {

    private static final long serialVersionUID = -4846734605399283040L;

    // ---------------------------------head field
    private String requestId;
    private Boolean success;
    private String business;
    private String errorCode;
    private String errorMessage;
    private String params;
    private Date date;
    private String version;

    // ---------------------------------data field
    private Integer tookTime;
    private Integer hitNum;
    private String name;

    public BaseResult() {}

    public BaseResult(BaseResult base) {
        if (base != null) {
            this.requestId = base.requestId;
            this.success = base.success;
            this.business = base.business;
            this.errorCode = base.errorCode;
            this.errorMessage = base.errorMessage;
            this.params = base.params;
            this.date = base.date;
            this.version = base.version;
            this.tookTime = base.tookTime;
            this.hitNum = base.hitNum;
        } else {
            this.success = false;
            this.date = new Date();
        }
    }

    public static BaseResult failure(String errorMsg) {
        return failure(null, errorMsg);
    }

    public static BaseResult failure(String errorCode, String errorMsg) {
        BaseResult result = new BaseResult();
        result.setSuccess(false);
        result.setErrorCode(errorCode);
        result.setErrorMessage(errorMsg);
        result.setDate(new Date());
        return result;
    }

    public static BaseResult success() {
        BaseResult result = new BaseResult();
        result.setSuccess(true);
        result.setDate(new Date());
        return result;
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public Boolean getSuccess() {
        return success;
    }

    public void setSuccess(Boolean success) {
        this.success = success;
    }

    public String getBusiness() {
        return business;
    }

    public void setBusiness(String business) {
        this.business = business;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(String errorCode) {
        this.errorCode = errorCode;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public String getParams() {
        return params;
    }

    public void setParams(String params) {
        this.params = params;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public Integer getTookTime() {
        return tookTime;
    }

    public void setTookTime(Integer tookTime) {
        this.tookTime = tookTime;
    }

    public Integer getHitNum() {
        return hitNum;
    }

    public void setHitNum(Integer hitNum) {
        this.hitNum = hitNum;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    // -------------------------------------------------------others
    @Transient
    public boolean isSuccess() {
        return Optional.ofNullable(success).orElse(false);
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this, SerializerFeature.DisableCircularReferenceDetect);
    }
}
