var objuser = document.getElementsByName("idrequest");
var objmsgfinal = document.getElementById("msg_final");
var objmsglog = document.getElementById("msg_log");
var g_msg = null;

function msg_log(p_in_msg){
        //alert(p_in_msg);
        console.log(p_in_msg);
        objmsgfinal.innerHTML = p_in_msg;
        objmsglog.innerHTML = p_in_msg;
}

function buscarLog(){
    var l_idrequest = "1234";
    if (l_idrequest === document.forms["buscarlog"]["idrequest"].value){
        msg_log("<i> Processando log python.</i>");         
        return true;
    }else {
        msg_log("<i> ID invalido!</i>");   
        return false;
        }
    
}

