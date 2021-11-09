<%@ page language="java" contentType="text/html; charset=UTF-8"
	pageEncoding="UTF-8"%>
<%@page import="cn.itcast.entity.User"%>
<jsp:useBean id="varDesc" scope="page" class="cn.itcast.entity.VarDesc"></jsp:useBean>
<%
	String path = request.getContextPath();
	String basePath = request.getScheme() + "://"
			+ request.getServerName() + ":" + request.getServerPort()
			+ path + "/";
	String viewId = request.getParameter("viewId");
%>
<%
	Object obj = request.getSession().getAttribute("user");
	if (!(obj instanceof User)) {
		response.sendRedirect(basePath + "timeout.jsp");
	}
%>
<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c"%>
<%@ taglib prefix="fmt" uri="http://java.sun.com/jsp/jstl/fmt"%>
<%@taglib uri="http://www.springframework.org/tags" prefix="spring"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<meta content="IE=edge,chrome=1" http-equiv="X-UA-Compatible">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta name="description" content="">
<meta name="author" content="">
<Link href="img/logo.ico" rel="Shortcut Icon">
<title>CNitcast</title>

<!-- 时间插件 -->
<link type="text/css"
	href="<%=basePath%>js/dateToSecond/css/jquery-ui-1.8.17.custom.css"
	rel="stylesheet" />
<link type="text/css"
	href="<%=basePath%>js/dateToSecond/css/jquery-ui-timepicker-addon.css"
	rel="stylesheet" />
<script type="text/javascript"
	src="<%=basePath%>js/dateToSecond/js/jquery-1.7.1.min.js"></script>
<script type="text/javascript"
	src="<%=basePath%>js/dateToSecond/js/jquery-ui-1.8.17.custom.min.js"></script>
<script type="text/javascript"
	src="<%=basePath%>js/dateToSecond/js/jquery-ui-timepicker-addon.js"></script>
<script type="text/javascript"
	src="<%=basePath%>js/dateToSecond/js/jquery-ui-timepicker-en-US.js"></script>

<!-- bootstrap加载 -->
<script type="text/javascript" src="<%=basePath%>js/bootstrap.js"></script>

<!-- 样式控制 -->
<link rel="stylesheet" type="text/css" href="<%=basePath%>css/theme.css">
<link type="text/css" href="<%=basePath%>css/conditions.css">
<link rel="stylesheet" type="text/css" href="<%=basePath%>css/bootstrap.css">
<!--  -->
<script type="text/javascript" src="<%=basePath%>css/layer/layer.min.js"></script>
<!--  -->
<!-- 加载层start -->
<script type="text/javascript" src="<%=basePath %>js/load.js"></script>
<!-- 加载层end -->
<script type="text/javascript">
/* 关闭父页面的遮罩层 */
window.parent.reqLoading(false);

	$(function() {
		var uls = $('.sidebar-nav > ul > *').clone();
		uls.addClass('visible-xs');
		$('#main-menu').append(uls.clone());
	});
	//调用时间插件
	 $(function () {
        $(".ui_timepicker").datetimepicker({
            //showOn: "button",
            //buttonImage: "./css/images/icon_calendar.gif",
            //buttonImageOnly: true,
            showSecond: true,
            timeFormat: 'hh:mm:ss',
            stepHour: 1,
            stepMinute: 1,
            stepSecond: 1
        })
    })
</script>
<style type="text/css">
.control-label {
	text-align: right;
	padding-top: 7px;
}

.btn-primary {
	width: 70px;
}

.onerowControl {
	display: inline;
	float: left;
}
.control1{
	width: 35%;
}
.control2-5{
	width: 14%;
}
.control6{
	width: 9%;
	text-align: center;
}
.label1{
	width: 14%; 
	float: left; 
	text-align: center;
	padding-top: 7px;
}
.label2-5{
	width: 50%; 
	text-align: center;
	padding-top: 7px;
}
.control1>input{
	width: 43%;
}
.control2-5>select{
	width: 50%;
}
</style>
<!-- Le HTML5 shim, for IE6-8 support of HTML5 elements -->
<!--[if lt IE 9]>
      <script src="http://html5shim.googlecode.com/svn/trunk/html5.js"></script>
    <![endif]-->


<!--[if lt IE 7 ]> <body class="ie ie6"> <![endif]-->
<!--[if IE 7 ]> <body class="ie ie7 "> <![endif]-->
<!--[if IE 8 ]> <body class="ie ie8 "> <![endif]-->
<!--[if IE 9 ]> <body class="ie ie9 "> <![endif]-->
<!--[if (gt IE 9)|!(IE)]><!-->

<!--<![endif]-->
</head>
<body class=" theme-blue">
	<div style="width: 100%; height: 100%;">
		<div class="header">
		<form action="<%=basePath%>appTraffic_appTrafficTracks">
			
			<div class="panel panel-default">
				<p class="panel-heading">
					<spring:message code="condition"></spring:message>
				</p>
				<div class="panel-body">
					<div class="row">
						<div>
							<div class="onerowControl control1">
								<label for="startDate" class="label1">
									<spring:message code="date"></spring:message>:
								</label>
								<input type="text"  id="startDate"  value="${startDateToSecond}" name="startDateToSecond"  class="onerowControl ui_timepicker form-control">
								<input type="text" id="endDate"     value="${endDateToSecond}" name="endDateToSecond"  class="onerowControl ui_timepicker form-control">
							</div>
							<div class="onerowControl control2-5">
								<label for="dateSize" class="onerowControl label2-5"><spring:message code="datesize"></spring:message></label>
								<select class="onerowControl form-control" name="x_type" type="text" id="dateSize">
									<c:choose>
											<c:when test="${x_type == varDesc.getAxisType('HOUR')}">
											<option value="hour" ><spring:message code="hour"></spring:message></option>
											<option value="day" ><spring:message code="day"></spring:message></option>
											<option value="month" ><spring:message code="month"></spring:message></option>
											</c:when>
											<c:when test="${x_type == varDesc.getAxisType('DAY')}">
											<option value="day" ><spring:message code="day"></spring:message></option>
											<option value="hour" ><spring:message code="hour"></spring:message></option>
											<option value="month" ><spring:message code="month"></spring:message></option>
											</c:when>
											<c:otherwise>
											<option value="month" ><spring:message code="month"></spring:message></option>
											<option value="day" ><spring:message code="day"></spring:message></option>
											<option value="hour" ><spring:message code="hour"></spring:message></option>
											</c:otherwise>
											</c:choose>
								</select>
							</div>
							<div class="onerowControl control2-5">
								<label for="landmark" class="onerowControl label2-5">
								<spring:message code="appflow"></spring:message></label>
								<select type="text" id="speed_type" name="speed_type"  class="form-control">
											<c:choose>
											<c:when test="${speedType == varDesc.getAppSpeedType('DLSPEED')}">
											
											
											<option value="ALL" selected="selected"><spring:message
													code="allflow"></spring:message></option>
											<option value="upload_traffic"><spring:message
													code="upflow"></spring:message></option>
											<option value="download_traffic">
											<spring:message code="downflow">
											</spring:message></option>
											</c:when>
											<c:when test="${speedType == varDesc.getAppSpeedType('ULSPEED')}">
											<option value="ALL" selected="selected"><spring:message
													code="allflow"></spring:message></option>
											<option value="upload_traffic"><spring:message
													code="upflow"></spring:message></option>
											<option value="download_traffic"><spring:message
													code="downflow"></spring:message></option>
											
											</c:when>
											<c:otherwise>
													<option value="ALL" selected="selected"><spring:message
													code="allflow"></spring:message></option>
											
											        <option value="upload_traffic"><spring:message
													code="upflow"></spring:message></option>
													<option value="download_traffic"><spring:message
													code="downflow"></spring:message></option>
											</c:otherwise>
											</c:choose>
											
									
										</select>
							</div>
							
							<div class="onerowControl control2-5">
								<label for="networkType" class="onerowControl label2-5"><spring:message
								code="operators"></spring:message></label>
								<c:choose>
								<c:when test="${sessionScope.user.role == varDesc.getRole('ADMIN')}">
								<select type="text" id="network_name" name="network_name" class="form-control">
											<c:choose>
											<c:when test="${nwOperator == varDesc.getOperator('CMCC')}">
											<option value="CMCC">CMCC</option>
											<option value="ALL">ALL</option>
											<option value="CTCC">CTCC</option>
											<option value="CUCC">CUCC</option>
											</c:when>
											<c:when test="${nwOperator == varDesc.getOperator('CTCC')}">
											<option value="CTCC">CTCC</option>
											<option value="ALL">ALL</option>
											<option value="CMCC">CMCC</option>
											<option value="CUCC">CUCC</option>
											</c:when>
											<c:when test="${nwOperator == varDesc.getOperator('CUCC')}">
											<option value="CUCC">CUCC</option>
											<option value="ALL">ALL</option>
											<option value="CMCC">CMCC</option>
											<option value="CTCC">CTCC</option>
											</c:when>
											<c:otherwise>
											<option value="ALL">ALL</option>
											<option value="CMCC">CMCC</option>
											<option value="CTCC">CTCC</option>
											<option value="CUCC">CUCC</option>
											</c:otherwise>
											</c:choose>
											</select>
								</c:when>
								<c:otherwise>
								<select type="text" id="network_name" name="network_name" class="form-control">
								<option value="${nwOperator}">${nwOperator}</option>
								</select>
								</c:otherwise>
								</c:choose>
							</div>
							
							<div class="onerowControl control2-5">
								<label for="networkType" class="onerowControl label2-5"><spring:message
								code="mobile_network_type"></spring:message></label>
								<select type="text" id="networkType" name="networkType" class="onerowControl form-control">
									<c:choose>
											<c:when test="${nwType == varDesc.getNetworkType('4G')}">
											<option value="4G" >4G</option>
											<option value="3G" >3G</option>
											<option value="2G" >2G</option>
											<option value="ALL" >ALL</option>
											</c:when>
											<c:when test="${nwType == varDesc.getNetworkType('3G')}">
											<option value="3G" >3G</option>
											<option value="4G" >4G</option>
											<option value="2G" >2G</option>
											<option value="ALL" >ALL</option>
											</c:when>
											<c:when test="${nwType == varDesc.getNetworkType('2G')}">
											<option value="2G" >2G</option>
											<option value="4G" >4G</option>
											<option value="3G" >3G</option>
											<option value="ALL" >ALL</option>
											</c:when>
											<c:otherwise>
											<option value="ALL" >ALL</option>
											<option value="4G" >4G</option>
											<option value="3G" >3G</option>
											<option value="2G" >2G</option>
											</c:otherwise>
											</c:choose>
								</select>
							</div>
							<div class="onerowControl control6">
								<button type="submit"  class="btn btn-danger" onclick="javascript:reqLoading(true);">
									<spring:message code="submit"></spring:message>
								</button>
							</div>
						</div>
					</div>
				</div>
			</div>
			</form>
		</div>

		<div class="main-content">
			<!-- 右侧上层图表 -->
			<c:choose>
			<c:when test="${optionStr==varDesc.getNodata()}">
			<div class="panel panel-default">
			
				<a href="#page-stats" class="panel-heading" data-toggle="collapse">
					<spring:message code="chart"></spring:message>
				</a>
				<div id="page-stats" class="panel-collapse panel-body collapse in">
					<div class="row">
						<div style="text-align:center;">没有数据!</div>
					</div>
				</div>
			</div>
			
			
			</c:when>
			<c:otherwise>
			<div class="panel panel-default">
			
				<a href="#page-stats" class="panel-heading" data-toggle="collapse">
					<spring:message code="chart"></spring:message>
				</a>
				<div id="page-stats" class="panel-collapse panel-body collapse in">
					<div class="row">
						<div id="main" style="height: 600px"></div>
					</div>
				</div>
			</div>
			</c:otherwise>
			</c:choose>
			</div>
	</div>

	<script src="lib/bootstrap/js/bootstrap.js"></script>
	<script src="<%=basePath%>js/echarts-all.js"></script>
	<script type="text/javascript">
		var networkQualityStatistics = echarts.init(document.getElementById('main'));
		var option = ${optionStr};
		 /* alert(option); */
		networkQualityStatistics.setOption(option);
	</script>

	
</html>